package entity

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/filter"
	"github.com/nisimpson/ezddb/operation"
)

// Relationship is formed between two [Entity] nodes together in a single direction,
// storing the start and end entity ids. Relationships between entities can
// be grouped together by relationship. An entity relationship with itself
// -- an "identity" relationship -- stores the target Entity info.
type Relationship struct {
	Entity
	StartEntityID string `dynamodbav:"gStartId"`      // The id of the starting Entity.
	EndEntityID   string `dynamodbav:"gEndId"`        // The id of the ending Entity.
	Relationship  string `dynamodbav:"gRelationship"` // The relationship between the two vertices.

	// The edge item type, which is equivalent to the Entity type
	// for identity edges, or a constant value labeled "edge" otherwise.
	itemType        string
	startEntityType string // The starting Entity type.
	endEntityType   string // The ending Entity type.
	isNode          bool   // true if the edge is an identity edge, or Entity node.
}

const (
	FilterRelationship  = Filter("data.gRelationship")
	FilterStartEntityID = Filter("data.gStartId")
	FilterEndEntityID   = Filter("data.gEndId")
)

func newIdentityRelationship(e Entity) Relationship {
	edge := newRelationship(e, e, "")
	edge.itemType = e.DynamoItemType()
	return edge
}

func newRelationship(start, end Entity, relationship string) Relationship {
	return Relationship{
		StartEntityID:   start.DynamoID(),
		EndEntityID:     end.DynamoID(),
		Relationship:    relationship,
		startEntityType: start.DynamoItemType(),
		endEntityType:   end.DynamoItemType(),
		itemType:        "relationship",
	}
}

// DynamoItemType returns the relationship type -- "relationship" for
// relationships between entities, or the start entity's item type.
func (e Relationship) DynamoItemType() string { return e.itemType }

// DynamoMarshalRecord implements [Entity], which can be stored as data records
// in the dynamodb [Table].
func (e Relationship) DynamoMarshalRecord(opts *MarshalOptions) {
	opts.HashKeyID = e.StartEntityID
	opts.SortKeyID = e.EndEntityID
	opts.HashKeyPrefix = e.startEntityType
	opts.SortKeyPrefix = e.endEntityType
	opts.SupportCollectionQuery = e.isNode
	opts.SupportReverseLookup = true

	if e.Relationship == "" {
		// ex, "customer/orders"
		opts.ReverseLookupSortKey = e.startEntityType + "/" + e.Relationship
	} else {
		// ex, "customer"
		opts.ReverseLookupSortKey = e.startEntityType
	}
}

// UnmarshalEntity extracts the [Entity] information from the specified
// item attribute map.
func UnmarshalEntity[V Entity](item ezddb.Item) (V, error) {
	var (
		Entity V
		record = Record[Relationship]{}
		err    = attributevalue.UnmarshalMap(item, &record)
	)

	if err == nil {
		Entity = record.Data.Entity.(V)
	}

	return Entity, err
}

// RelationshipUnmarshaler can extract edge information from dynamodb items,
// including the relationship name, starting Entity id, and ending
// Entity id.
type RelationshipUnmarshaler interface {
	// UnmarshalEdge extracts the edge information from the specified
	// item attribute map.
	//
	// The relationship name, start Entity id, and end Entity id are
	// extracted from the item and passed to the method.
	//
	// The method should return an error if the item does not contain
	// the expected edge information.
	UnmarshalEdge(relationship string, startID, endID string) error
}

// UnmarshalRelationship extracts the edge information from the specified
// item attribute map.
//
// The relationship name, start Entity id, and end Entity id are
// extracted from the item and passed to the [RelationshipUnmarshaler].
func UnmarshalRelationship[V RelationshipUnmarshaler](item ezddb.Item, out RelationshipUnmarshaler) error {
	var (
		record = Record[Relationship]{}
		err    = attributevalue.UnmarshalMap(item, &record)
	)

	if err == nil {
		err = out.UnmarshalEdge(
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
	Table[Relationship]
}

// New creates a new [Graph] client.
func New(tableName string, opts ...func(*Options)) Graph {
	return Graph{
		Table: NewTable[Relationship](tableName, opts...),
	}
}

// AddEntity adds a new identity [Relationship] associated with the target [Entity] to the table.
func (g Graph) AddEntity(e Entity, opts ...func(*Options)) operation.Put {
	g.options.apply(opts)
	return g.Put(newIdentityRelationship(e))
}

// GetEntity retrieves the identity [Relationship] associated with the target [Entity] id.
func (g Graph) GetEntity(v Entity, opts ...func(*Options)) operation.Get {
	g.options.apply(opts)
	return g.Get(newIdentityRelationship(v))
}

// GetRelationship retrieves the [Relationship] between the start and end [Entity] nodes.
func (g Graph) GetRelationship(start, end Entity, opts ...func(*Options)) operation.Get {
	g.options.apply(opts)
	return g.Get(newRelationship(start, end, ""))
}

// DeleteEntity removes the identity [Relationship] associated with the target [Entity].
func (g Graph) DeleteEntity(v Entity, opts ...func(*Options)) operation.Delete {
	g.options.apply(opts)
	return g.Delete(newIdentityRelationship(v))
}

// ListEntitiesQuery contains options for searching through the list of entities
// within the [Graph] table.
type ListEntitiesQuery struct {
	QueryOptions
	Options []func(*Options)
}

// ListEntities searches for and returns a list entities of the same entity type within the [Graph].
// Modify or extend the query options using a [ListEntitiesQuery] function.
func (g Graph) ListEntities(itemType string, opts ...func(ListEntitiesQuery)) operation.Query {
	query := ListEntitiesQuery{}
	query.PartitionKeyValue = itemType
	for _, o := range opts {
		o(query)
	}
	return g.Query(CollectionQuery{QueryOptions: query.QueryOptions}, query.Options...)
}

// EntityWithRelationships provides methods for defining the relationships an [Entity]
// has with other entities.
type EntityWithRelationships interface {
	Entity
	// DynamoRelationships returns the list of relationship names defined by an [Entity].
	DynamoRelationships() []string
	// DynamoIsReverseRelationship returns true if the relationship associated with the
	// specified name is stored on the "end" side of the relationship. Reverse relationships
	// are optimal for "one-to-many" or "many-to-many" relationships, where a single reference
	// to the "start" entity is stored on the "end" entity's partition. This association can be
	// queried with the [Graph.ListRelationships] method, with the [ListRelationshipsQuery.Reverse]
	// field set to true.
	DynamoIsReverseRelationship(name string) bool
	// DynamoGetRelationship returns the list of entities that comprise the specified relationship.
	DynamoGetRelationship(name string) []Entity
}

// AddRelationships adds the relationships defined by the target [EntityWithRelationships].
func (g Graph) AddRelationships(e EntityWithRelationships, opts ...func(*Options)) operation.BatchWriteItemCollection {
	g.options.apply(opts)
	collection := make(operation.BatchWriteItemCollection, 0)
	for _, name := range e.DynamoRelationships() {
		entities := e.DynamoGetRelationship(name)
		for _, entity := range entities {
			var relationship Relationship
			if e.DynamoIsReverseRelationship(name) {
				relationship = newRelationship(e, entity, name)
			} else {
				relationship = newRelationship(entity, e, name)
			}
			collection = append(collection, g.Put(relationship))
		}
	}

	return collection
}

// DeleteRelationship removes the [Relationship] items associated with the target name.
func (g Graph) DeleteRelationships(e EntityWithRelationships, name string, opts ...func(*Options)) operation.BatchWriteItemCollection {
	g.options.apply(opts)
	var (
		refs    = e.DynamoGetRelationship(name)
		reverse = e.DynamoIsReverseRelationship(name)
		batch   = make(operation.BatchWriteItemCollection, 0, len(refs))
	)

	for _, r := range refs {
		var ref Relationship
		if reverse {
			ref = newRelationship(r, e, name)
		} else {
			ref = newRelationship(e, r, name)
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
	Options      []func(*Options)
}

// ListRelationships generates a query that searches for relationships formed by the specified [EntityWithRelationships].
// Modify or extend the query options with a [ListRelationshipsQuery] modifier.
func (g Graph) ListRelationships(e EntityWithRelationships, opts ...func(ListRelationshipsQuery)) operation.Query {
	var (
		query = ListRelationshipsQuery{
			QueryOptions: QueryOptions{
				Filter: filter.Exists(FilterHK.Attribute()).Condition(), // something that is always true
			},
		}
	)

	for _, o := range opts {
		o(query)
	}

	var (
		node     = Marshal(newIdentityRelationship(e))
		edge     = Marshal(newRelationship(e, e, query.Relationship))
		strategy QueryStrategy
	)

	if query.Reverse {
		lookup := ReverseLookupQuery{QueryOptions: query.QueryOptions}
		lookup.PartitionKeyValue = node.SK
		lookup.SortKeyPrefix = edge.GSI1SK
		strategy = lookup
	} else {
		lookup := LookupQuery{QueryOptions: query.QueryOptions}
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
