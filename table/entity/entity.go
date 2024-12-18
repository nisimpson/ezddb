package entity

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/query"
	"github.com/nisimpson/ezddb/stored"
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

type Relationship struct {
	// True if the relationship is an "one-to-many" relationship, which are
	// are stored on the "end" side of the relationship. Reverse relationships
	// are optimal for "one-to-many" or "many-to-many" relationships, where a single reference
	// to the "start" entity is stored on the "end" entity's partition. This association can be
	// queried with the [Graph.ListRelationships] method, with the [ListRelationshipsQuery.Reverse]
	// field set to true.
	IsOneToMany bool
	// The sort key to use for the relationship. If empty, the sort key will be a combination
	// of the start and end entity types.
	//
	// Bi-directional relationships should share the same relationship name and sort key
	// for consistency:
	//
	//	// Relationship.Name = "customer-order"
	//	// Relationship.IsMany = true
	//	// Relationship.SortKey = "customer/orders"
	//	type Customer struct { Orders []*Order }
	//
	//	// Relationship.Name = "customer-order"
	//	// Relationship.IsMany = false
	//	// Relationship.SortKey = "customer/orders"
	//	type Order struct { Customer *Customer }
	//
	// This ensures that the target relationships are stored on the same partition.
	SortKey string
}

// DataWithRelationships provides methods for defining the relationships an [Data]
// has with other entities.
type DataWithRelationships interface {
	Data
	// EntityRelationships returns the list of relationship names defined by an [Entity].
	// For example, a Customer -> Order one-to-many relationship could have the name "orders".
	EntityRelationships() map[string]Relationship
	// EntityRef returns the list of entities that comprise the specified relationship.
	EntityRef(name string) []Data
}

// Ref returns a slice of [Data], serving as a helper function for creating
// and return the correct data type for [DataWithRelationships.EntityRelationship].
func Ref[T Data](s ...T) []Data {
	arr := make([]Data, len(s))
	for i, v := range s {
		arr[i] = v
	}
	return arr
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
	AttributeRelationship  = query.Attribute("data", "gRelationship")
	AttributeStartEntityID = query.Attribute("data", "gStartId")
	AttributeEndEntityID   = query.Attribute("data", "gEndId")
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

// RefUnmarshaler can extract edge information from dynamodb items,
// including the relationship name, starting Entity id, and ending
// Entity id.
type RefUnmarshaler interface {
	// UnmarshalRelationship extracts the edge information from the specified
	// item attribute map.
	//
	// The relationship name, start Entity id, and end Entity id are
	// extracted from the item and passed to the method.
	//
	// The method should return an error if the item does not contain
	// the expected edge information.
	UnmarshalEntityRef(name string, startID, endID string) error
}

// UnmarshalEntityRef extracts the edge information from the specified
// item attribute map.
//
// The relationship name, start Entity id, and end Entity id are
// extracted from the item and passed to the [RefUnmarshaler].
func UnmarshalEntityRef[T RefUnmarshaler](item ezddb.Item, out T) error {
	var (
		record = table.Record[record[Data]]{}
		err    = attributevalue.UnmarshalMap(item, &record)
	)

	if err == nil {
		err = out.UnmarshalEntityRef(
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
func (g Graph) AddEntity(e Data, opts ...func(*table.Options)) stored.Put {
	g.Options.Apply(opts)
	return g.Put(newIdentityRelationship(e))
}

// GetEntity retrieves the identity [relationship] associated with the target [Data] id.
func (g Graph) GetEntity(e Data, opts ...func(*table.Options)) stored.Get {
	g.Options.Apply(opts)
	return g.Get(newIdentityRelationship(e))
}

// GetRelationship retrieves the [relationship] between the start and end [Data] nodes.
func (g Graph) GetRelationship(start DataWithRelationships, end Data, name string, opts ...func(*table.Options)) stored.Get {
	g.Options.Apply(opts)
	var (
		defs = start.EntityRelationships()
	)
	if defs[name].IsOneToMany {
		return g.Get(newReverseRelationship(start, end, "", ""))
	}
	return g.Get(newRelationship(start, end, "", ""))
}

// DeleteEntity removes the identity [relationship] associated with the target [Data].
func (g Graph) DeleteEntity(v Data, opts ...func(*table.Options)) stored.Delete {
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
func (g Graph) ListEntities(itemType string, opts ...func(*ListEntitiesQuery)) stored.Query {
	q := ListEntitiesQuery{}
	q.PartitionKeyValue = itemType
	q.Filter = query.Identity().Condition()

	for _, o := range opts {
		o(&q)
	}
	return g.Query(table.CollectionQuery{QueryOptions: q.QueryOptions}, q.Options...)
}

// AddRelationships adds the relationships defined by the target [DataWithRelationships].
func (g Graph) AddRelationships(e DataWithRelationships, opts ...func(*table.Options)) stored.BatchWriteItemCollection {
	g.Options.Apply(opts)
	collection := make(stored.BatchWriteItemCollection, 0)
	for name, ref := range e.EntityRelationships() {
		var (
			entities       = e.EntityRef(name)
			relationshipSK = ref.SortKey
			reverse        = ref.IsOneToMany
		)
		for _, ref := range entities {
			var relationship record[Data]
			if reverse {
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
func (g Graph) DeleteRelationship(e DataWithRelationships, name string, opts ...func(*table.Options)) stored.BatchWriteItemCollection {
	g.Options.Apply(opts)
	var (
		defs    = e.EntityRelationships()
		refs    = e.EntityRef(name)
		reverse = defs[name].IsOneToMany
		sortKey = defs[name].SortKey
		batch   = make(stored.BatchWriteItemCollection, 0, len(refs))
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

// ListRelationships generates a query that searches for relationships formed by the specified [DataWithRelationships].
// Modify or extend the query options with a [ListRelationshipsQuery] modifier.
func (g Graph) ListRelationships(e DataWithRelationships, opts ...func(*ListRelationshipsQuery)) stored.Query {
	var (
		node     = table.MarshalRecord(newIdentityRelationship(e))
		defs     = e.EntityRelationships()
		strategy table.QueryStrategy
	)

	var (
		q = ListRelationshipsQuery{
			QueryOptions: table.QueryOptions{
				Filter: query.Identity().Condition(),
			},
		}
	)

	for _, o := range opts {
		o(&q)
	}

	if q.Reverse {
		lookup := table.ReverseLookupQuery{QueryOptions: q.QueryOptions}
		lookup.PartitionKeyValue = node.SK
		if q.Relationship != "" {
			def := defs[q.Relationship]
			lookup.SortKeyPrefix = def.SortKey
		}
		strategy = lookup
	} else {
		lookup := table.LookupQuery{QueryOptions: q.QueryOptions}
		lookup.PartitionKeyValue = node.HK
		if q.Relationship != "" {
			lookup.Filter = lookup.Filter.And(
				query.HasSubstring(
					AttributeRelationship,
					q.Relationship).
					Condition(),
			)
		}
		strategy = lookup
	}

	return g.Query(strategy, q.Options...)
}

// EntityAttribute returns a [Filter] that runs conditions on the embedded [Entity]
func (Graph) EntityAttribute(name string) query.ItemAttribute {
	return query.Attribute("data", "value", name)
}
