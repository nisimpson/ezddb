package entity

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/filter"
	"github.com/nisimpson/ezddb/operation"
	"github.com/nisimpson/ezddb/table"
)

// Data represents a singular "thing" within an entity-relationship model.
// Data entities can form relationships with other entities; these associations
// are stored within the dynamodb table as separate rows within the table.
// Use the [Graph] client to perform CRUD operations on stored entities.
type Data interface {
	EntityID() string
	EntityType() string
}

// record stores information about two entities in a relationship,
// referenced by the start and end entity ids. Relationships between entities are
// either grouped together on the start entity's partition, or on the end entity's
// partition as a "reverse" relationship. A record with equivalent
// start and end ids is called an "identity" record and stores the target entity data.
type record[T Data] struct {
	Value         T      `dynamodbav:"value"`         // The record value contain entityh data.
	StartEntityID string `dynamodbav:"gStartId"`      // The id of the starting entity.
	EndEntityID   string `dynamodbav:"gEndId"`        // The id of the ending entity.
	Relationship  string `dynamodbav:"gRelationship"` // The relationship between the two vertices.

	// The edge item type, which is equivalent to the Entity type
	// for identity edges, or a constant value labeled "edge" otherwise.
	itemType        string
	startEntityType string // The starting Entity type.
	endEntityType   string // The ending Entity type.
	identity        bool   // true if the edge is an identity edge, or Entity node.
	reverse         bool   // true if the relationship is a reverse relationship.
	relationshipSK  string
}

var (
	AttributeRelationship  = filter.AttributeOf("data", "gRelationship")
	AttributeStartEntityID = filter.AttributeOf("data", "gStartId")
	AttributeEndEntityID   = filter.AttributeOf("data", "gEndId")
)

func newIdentityRelationship(e Data) record[Data] {
	edge := newRelationship(e, e, "", e.EntityType())
	edge.itemType = e.EntityType()
	edge.identity = true
	edge.Value = e
	return edge
}

func newRelationship(start, end Data, name string, relationshipSK string) record[Data] {
	return record[Data]{
		StartEntityID:   start.EntityID(),
		EndEntityID:     end.EntityID(),
		Relationship:    name,
		startEntityType: start.EntityType(),
		endEntityType:   end.EntityType(),
		itemType:        "relationship",
		relationshipSK:  relationshipSK,
	}
}

func newReverseRelationship(start, end Data, name string, relationshipSK string) record[Data] {
	edge := newRelationship(end, start, name, relationshipSK)
	edge.reverse = true
	return edge
}

// DynamoMarshalRecord implements [Data], which can be stored as data records
// in the dynamodb [Table].
func (e record[T]) DynamoMarshalRecord(opts *table.MarshalOptions) {
	opts.HashKeyID = e.StartEntityID
	opts.SortKeyID = e.EndEntityID
	opts.HashKeyPrefix = e.startEntityType
	opts.SortKeyPrefix = e.endEntityType
	opts.ItemType = e.itemType
	opts.SupportCollectionQuery = e.identity
	opts.SupportReverseLookup = true
	opts.ReverseLookupSortKey = e.relationshipSK
}

// UnmarshalEntity extracts the [Data] information from the specified
// item attribute map.
func UnmarshalEntity[T Data](item ezddb.Item) (T, error) {
	var (
		data   T
		record = table.Record[record[T]]{}
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
		record = table.Record[record[Data]]{}
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
	table.Table[record[Data]]
}

// NewGraph creates a new [Graph] client.
func NewGraph(tableName string, opts ...func(*table.Options)) Graph {
	return Graph{
		Table: table.New[record[Data]](tableName, opts...),
	}
}

// AddEntity adds a new identity [relationship] associated with the target [Data] to the table.
func (g Graph) AddEntity(e Data, opts ...func(*table.Options)) operation.Put {
	g.Options.Apply(opts)
	return g.Put(newIdentityRelationship(e))
}

// GetEntity retrieves the identity [relationship] associated with the target [Data] id.
func (g Graph) GetEntity(e Data, opts ...func(*table.Options)) operation.Get {
	g.Options.Apply(opts)
	return g.Get(newIdentityRelationship(e))
}

// GetRelationship retrieves the [relationship] between the start and end [Data] nodes.
func (g Graph) GetRelationship(start EntityWithRelationships, end Data, name string, opts ...func(*table.Options)) operation.Get {
	g.Options.Apply(opts)
	if start.EntityIsReverseRelationship(name) {
		return g.Get(newReverseRelationship(start, end, "", ""))
	}
	return g.Get(newRelationship(start, end, "", ""))
}

// DeleteEntity removes the identity [relationship] associated with the target [Data].
func (g Graph) DeleteEntity(v Data, opts ...func(*table.Options)) operation.Delete {
	g.Options.Apply(opts)
	return g.Delete(newIdentityRelationship(v))
}

// ListEntitiesQuery contains options for searching through the list of entities
// within the [Graph] table.
type ListEntitiesQuery struct {
	table.QueryOptions
	Options []func(*table.Options)
}

// ListEntities searches for and returns a list entities of the same entity type within the [Graph].
// Modify or extend the query options using a [ListEntitiesQuery] function.
func (g Graph) ListEntities(itemType string, opts ...func(*ListEntitiesQuery)) operation.Query {
	query := ListEntitiesQuery{}
	query.PartitionKeyValue = itemType
	query.Filter = filter.Identity().Condition()

	for _, o := range opts {
		o(&query)
	}
	return g.Query(table.CollectionQuery{QueryOptions: query.QueryOptions}, query.Options...)
}

// EntityWithRelationships provides methods for defining the relationships an [Data]
// has with other entities.
type EntityWithRelationships interface {
	Data
	// EntityRelationships returns the list of relationship names defined by an [Entity].
	// For example, a Customer -> Order one-to-many relationship could have the name "orders".
	EntityRelationships() []string
	// EntityIsReverseRelationship returns true if the relationship associated with the
	// specified name is stored on the "end" side of the relationship. Reverse relationships
	// are optimal for "one-to-many" or "many-to-many" relationships, where a single reference
	// to the "start" entity is stored on the "end" entity's partition. This association can be
	// queried with the [Graph.ListRelationships] method, with the [ListRelationshipsQuery.Reverse]
	// field set to true.
	EntityIsReverseRelationship(name string) bool
	// EntityRelationship returns the list of entities that comprise the specified relationship.
	EntityRelationship(name string) []Data
	// EntityRelationshipSortKey returns the common sort key that is used for the target
	// relationship. Bi-directional relationships should share the same relationship and sort key:
	//
	//	// EntityType() = "customer"
	//	// EntityIsReverseRelationship("customer-order") = true
	//	// EntityRelationshipSortKey("customer-order") = "customer/orders"
	//	type Customer struct { Orders []*Order }
	//
	//	// EntityType() = "order"
	//	// EntityIsReverseRelationship("customer-order") = false
	//	// EntityRelationshipSortKey("customer-order") = "customer/orders"
	//	type Order struct { Customer *Customer }
	//
	// This ensures that the target relationship is stored consistently.
	EntityRelationshipSortKey(name string) string
}

// AddRelationships adds the relationships defined by the target [EntityWithRelationships].
func (g Graph) AddRelationships(e EntityWithRelationships, opts ...func(*table.Options)) operation.BatchWriteItemCollection {
	g.Options.Apply(opts)
	collection := make(operation.BatchWriteItemCollection, 0)
	for _, name := range e.EntityRelationships() {
		var (
			entities       = e.EntityRelationship(name)
			relationshipSK = e.EntityRelationshipSortKey(name)
		)
		for _, ref := range entities {
			var relationship record[Data]
			if e.EntityIsReverseRelationship(name) {
				relationship = newReverseRelationship(e, ref, name, relationshipSK)
			} else {
				relationship = newRelationship(e, ref, name, relationshipSK)
			}
			collection = append(collection, g.Put(relationship))
		}
	}

	return collection
}

// DeleteRelationship removes the [relationship] items associated with the target name.
func (g Graph) DeleteRelationship(e EntityWithRelationships, name string, opts ...func(*table.Options)) operation.BatchWriteItemCollection {
	g.Options.Apply(opts)
	var (
		refs    = e.EntityRelationship(name)
		reverse = e.EntityIsReverseRelationship(name)
		sortKey = e.EntityRelationshipSortKey(name)
		batch   = make(operation.BatchWriteItemCollection, 0, len(refs))
	)

	for _, r := range refs {
		var ref record[Data]
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
	table.QueryOptions
	Reverse      bool   // If true, performs a reverse lookup on the target entity.
	Relationship string // The target relationship name. A zero value will return all relationships.
	Options      []func(*table.Options)
}

// ListRelationships generates a query that searches for relationships formed by the specified [EntityWithRelationships].
// Modify or extend the query options with a [ListRelationshipsQuery] modifier.
func (g Graph) ListRelationships(e EntityWithRelationships, opts ...func(*ListRelationshipsQuery)) operation.Query {
	var (
		node     = table.MarshalRecord(newIdentityRelationship(e))
		strategy table.QueryStrategy
	)

	var (
		query = ListRelationshipsQuery{
			QueryOptions: table.QueryOptions{
				Filter: filter.Identity().Condition(),
			},
		}
	)

	for _, o := range opts {
		o(&query)
	}

	if query.Reverse {
		lookup := table.ReverseLookupQuery{QueryOptions: query.QueryOptions}
		lookup.PartitionKeyValue = node.SK
		if query.Relationship != "" {
			lookup.SortKeyPrefix = e.EntityRelationshipSortKey(query.Relationship)
		}
		strategy = lookup
	} else {
		lookup := table.LookupQuery{QueryOptions: query.QueryOptions}
		lookup.PartitionKeyValue = node.HK
		if query.Relationship != "" {
			lookup.Filter = lookup.Filter.And(
				filter.HasSubstring(
					AttributeRelationship,
					query.Relationship).
					Condition(),
			)
		}
		strategy = lookup
	}

	return g.Query(strategy, query.Options...)
}

// EntityAttribute returns a [Filter] that runs conditions on the embedded [Entity]
func (Graph) EntityAttribute(name string) filter.Attribute {
	return filter.AttributeOf("data", "value", name)
}
