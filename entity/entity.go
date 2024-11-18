package entity

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/filter"
	"github.com/nisimpson/ezddb/operation"
)

// Relationship is formed between two [Data] nodes together in a single direction,
// storing the start and end entity ids. Relationships between entities can
// be grouped together by relationship. An entity relationship with itself
// -- an "identity" relationship -- stores the target Entity info.
type Relationship[T Data] struct {
	Value T `dynamodbav:"value"`

	StartEntityID string `dynamodbav:"gStartId"`      // The id of the starting Entity.
	EndEntityID   string `dynamodbav:"gEndId"`        // The id of the ending Entity.
	Relationship  string `dynamodbav:"gRelationship"` // The relationship between the two vertices.

	// The edge item type, which is equivalent to the Entity type
	// for identity edges, or a constant value labeled "edge" otherwise.
	itemType        string
	startEntityType string // The starting Entity type.
	endEntityType   string // The ending Entity type.
	isNode          bool   // true if the edge is an identity edge, or Entity node.
	isReverse       bool   // true if the relationship is a reverse relationship.
	relationshipSK  string
}

const (
	FilterRelationship  = Filter("data.gRelationship")
	FilterStartEntityID = Filter("data.gStartId")
	FilterEndEntityID   = Filter("data.gEndId")
)

func newIdentityRelationship(e Data) Relationship[Data] {
	edge := newRelationship(e, e, "", e.DynamoItemType())
	edge.itemType = e.DynamoItemType()
	edge.isNode = true
	edge.Value = e
	return edge
}

func newRelationship(start, end Data, relationship string, relationshipSK string) Relationship[Data] {
	return Relationship[Data]{
		StartEntityID:   start.DynamoID(),
		EndEntityID:     end.DynamoID(),
		Relationship:    relationship,
		startEntityType: start.DynamoItemType(),
		endEntityType:   end.DynamoItemType(),
		itemType:        "relationship",
		relationshipSK:  relationshipSK,
	}
}

func newReverseRelationship(start, end Data, relationship string, relationshipSK string) Relationship[Data] {
	edge := newRelationship(end, start, relationship, relationshipSK)
	edge.isReverse = true
	return edge
}

// DynamoID returns the start entity id.
func (e Relationship[T]) DynamoID() string { return e.StartEntityID }

// DynamoItemType returns the relationship type -- "relationship" for
// relationships between entities, or the start entity's item type.
func (e Relationship[T]) DynamoItemType() string { return e.itemType }

// DynamoMarshalRecord implements [Data], which can be stored as data records
// in the dynamodb [Table].
func (e Relationship[T]) DynamoMarshalRecord(opts *MarshalOptions) {
	opts.HashKeyID = e.StartEntityID
	opts.SortKeyID = e.EndEntityID
	opts.HashKeyPrefix = e.startEntityType
	opts.SortKeyPrefix = e.endEntityType
	opts.SupportCollectionQuery = e.isNode
	opts.SupportReverseLookup = true
	opts.ReverseLookupSortKey = e.relationshipSK
}

// UnmarshalEntity extracts the [Data] information from the specified
// item attribute map.
func UnmarshalEntity[T Data](item ezddb.Item) (T, error) {
	var (
		data   T
		record = Record[Relationship[T]]{}
		err    = attributevalue.UnmarshalMap(item, &record)
	)

	if err == nil {
		data = record.Data.Value
	}

	return data, err
}

// RelationshipUnmarshaler can extract edge information from dynamodb items,
// including the relationship name, starting Entity id, and ending
// Entity id.
type RelationshipUnmarshaler interface {
	// UnmarshalRelationship extracts the edge information from the specified
	// item attribute map.
	//
	// The relationship name, start Entity id, and end Entity id are
	// extracted from the item and passed to the method.
	//
	// The method should return an error if the item does not contain
	// the expected edge information.
	UnmarshalRelationship(name string, startID, endID string) error
}

// UnmarshalRelationship extracts the edge information from the specified
// item attribute map.
//
// The relationship name, start Entity id, and end Entity id are
// extracted from the item and passed to the [RelationshipUnmarshaler].
func UnmarshalRelationship[T RelationshipUnmarshaler](item ezddb.Item, out T) error {
	var (
		record = Record[Relationship[Data]]{}
		err    = attributevalue.UnmarshalMap(item, &record)
	)

	if err == nil {
		err = out.UnmarshalRelationship(
			record.Data.Relationship,
			record.Data.StartEntityID,
			record.Data.EndEntityID,
		)
	}

	return err
}

// Graph clients access a dynamodb table as a graph of entities
// and the relationships between them. Each item in the table represents
// a relationship between two entities. The item storing the "identity"
// relationship -- an entity related to itself -- also stores the entity's
// attributes. Otherwise, only identifier data is stored in the item.
//
// Nodes in the graph are represented by table partition; in this way, an entity node
// can be seen as the composition of its relationships. Information about the node
// is stored in the "identity edge", or dynamodb item with an equivalent
// hash and sort key.
//
// Edge direction is represented by the partition where the item
// is stored; Given nodes A and B, edge A -> B is stored on partition A
// and is queried on node A. In order to query the reverse edge B -> A,
// Graph uses the reverse lookup index on node B.
type Graph struct {
	Table[Relationship[Data]]
}

// New creates a new [Graph] client.
func New(tableName string, opts ...func(*TableOptions)) Graph {
	return Graph{
		Table: NewTable[Relationship[Data]](tableName, opts...),
	}
}

// AddEntity adds a new identity [Relationship] associated with the target [Data] to the table.
func (g Graph) AddEntity(e Data, opts ...func(*TableOptions)) operation.Put {
	g.Options.apply(opts)
	return g.Put(newIdentityRelationship(e))
}

// GetEntity retrieves the identity [Relationship] associated with the target [Data] id.
func (g Graph) GetEntity(e Data, opts ...func(*TableOptions)) operation.Get {
	g.Options.apply(opts)
	return g.Get(newIdentityRelationship(e))
}

// GetRelationship retrieves the [Relationship] between the start and end [Data] nodes.
func (g Graph) GetRelationship(start, end Data, opts ...func(*TableOptions)) operation.Get {
	g.Options.apply(opts)
	return g.Get(newRelationship(start, end, "", ""))
}

// DeleteEntity removes the identity [Relationship] associated with the target [Data].
func (g Graph) DeleteEntity(v Data, opts ...func(*TableOptions)) operation.Delete {
	g.Options.apply(opts)
	return g.Delete(newIdentityRelationship(v))
}

// ListEntitiesQuery contains options for searching through the list of entities
// within the [Graph] table.
type ListEntitiesQuery struct {
	QueryOptions
	Options []func(*TableOptions)
}

// ListEntities searches for and returns a list entities of the same entity type within the [Graph].
// Modify or extend the query options using a [ListEntitiesQuery] function.
func (g Graph) ListEntities(itemType string, opts ...func(*ListEntitiesQuery)) operation.Query {
	query := ListEntitiesQuery{}
	query.PartitionKeyValue = itemType
	for _, o := range opts {
		o(&query)
	}
	return g.Query(CollectionQuery{QueryOptions: query.QueryOptions}, query.Options...)
}

// EntityWithRelationships provides methods for defining the relationships an [Data]
// has with other entities.
type EntityWithRelationships interface {
	Data
	// DynamoRelationships returns the list of relationship names defined by an [Entity].
	// For example, a Customer -> Order one-to-many relationship could have the name "orders".
	DynamoRelationships() []string
	// DynamoIsReverseRelationship returns true if the relationship associated with the
	// specified name is stored on the "end" side of the relationship. Reverse relationships
	// are optimal for "one-to-many" or "many-to-many" relationships, where a single reference
	// to the "start" entity is stored on the "end" entity's partition. This association can be
	// queried with the [Graph.ListRelationships] method, with the [ListRelationshipsQuery.Reverse]
	// field set to true.
	DynamoIsReverseRelationship(name string) bool
	// DynamoGetRelationship returns the list of entities that comprise the specified relationship.
	DynamoGetRelationship(name string) []Data
	// DynamoGetRelationshipSortKey returns the common sort key that is used for the target
	// relationship. Bi-directional relationships should share the same sort key:
	//
	//	// DynamoItemType() = "customer"
	//	// DynamoIsReverseRelationship("orders") = true
	//	// DynamoGetRelationshipSortKey("orders") = "customer/orders"
	//	type Customer struct { Orders []*Order }
	//
	//	// DynamoItemType() = "order"
	//	// DynamoIsReverseRelationship("customer") = false
	//	// DynamoGetRelationshipSortKey("customer") = "customer/orders"
	//	type Order struct { Customer *Customer }
	//
	// This ensures that the target relationship is stored consistently.
	DynamoGetRelationshipSortKey(name string) string
}

// AddRelationships adds the relationships defined by the target [EntityWithRelationships].
func (g Graph) AddRelationships(e EntityWithRelationships, opts ...func(*TableOptions)) operation.BatchWriteItemCollection {
	g.Options.apply(opts)
	collection := make(operation.BatchWriteItemCollection, 0)
	for _, name := range e.DynamoRelationships() {
		var (
			entities       = e.DynamoGetRelationship(name)
			relationshipSK = e.DynamoGetRelationshipSortKey(name)
		)
		for _, ref := range entities {
			var relationship Relationship[Data]
			if e.DynamoIsReverseRelationship(name) {
				relationship = newReverseRelationship(e, ref, name, relationshipSK)
			} else {
				relationship = newRelationship(e, ref, name, relationshipSK)
			}
			collection = append(collection, g.Put(relationship))
		}
	}

	return collection
}

// DeleteRelationship removes the [Relationship] items associated with the target name.
func (g Graph) DeleteRelationships(e EntityWithRelationships, name string, opts ...func(*TableOptions)) operation.BatchWriteItemCollection {
	g.Options.apply(opts)
	var (
		refs    = e.DynamoGetRelationship(name)
		reverse = e.DynamoIsReverseRelationship(name)
		sortKey = e.DynamoGetRelationshipSortKey(name)
		batch   = make(operation.BatchWriteItemCollection, 0, len(refs))
	)

	for _, r := range refs {
		var ref Relationship[Data]
		if reverse {
			ref = newReverseRelationship(e, r, name, sortKey)
		} else {
			ref = newRelationship(e, r, name, sortKey)
		}
		batch = append(batch, g.Delete(ref))
	}

	return batch
}

// ListRelationshipsQuery contains options for listing an entity's relationships.
type ListRelationshipsQuery struct {
	QueryOptions
	Reverse      bool   // If true, performs a reverse lookup on the target entity.
	Relationship string // The target relationship name. A zero value will return all relationships.
	Options      []func(*TableOptions)
}

// ListRelationships generates a query that searches for relationships formed by the specified [EntityWithRelationships].
// Modify or extend the query options with a [ListRelationshipsQuery] modifier.
func (g Graph) ListRelationships(e EntityWithRelationships, opts ...func(*ListRelationshipsQuery)) operation.Query {
	var (
		query = ListRelationshipsQuery{
			QueryOptions: QueryOptions{
				Filter: filter.Exists(FilterHK.Attribute()).Condition(), // something that is always true
			},
		}
	)

	for _, o := range opts {
		o(&query)
	}

	var (
		node     = Marshal(newIdentityRelationship(e))
		strategy QueryStrategy
	)

	if query.Reverse {
		lookup := ReverseLookupQuery{QueryOptions: query.QueryOptions}
		lookup.PartitionKeyValue = node.SK
		if query.Relationship != "" {
			lookup.SortKeyPrefix = e.DynamoGetRelationshipSortKey(query.Relationship)
		}
		strategy = lookup
	} else {
		lookup := LookupQuery{QueryOptions: query.QueryOptions}
		lookup.PartitionKeyValue = node.HK
		if query.Relationship != "" {
			lookup.Filter = lookup.Filter.And(
				filter.HasSubstring(
					FilterRelationship.Attribute(),
					query.Relationship).
					Condition(),
			)
		}
		strategy = lookup
	}

	return g.Query(strategy, query.Options...)
}

// DataFilter returns a [Filter] that runs conditions on the embedded [Entity]
func (Graph) DataFilter(name string) Filter {
	return Filter("data.value." + name)
}
