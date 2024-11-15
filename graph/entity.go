package graph

import (
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/filter"
	"github.com/nisimpson/ezddb/operation"
)

// Entity represents a singular "thing" within an entity-relationship model.
// Entities can form relationships with other entities; these associations
// are stored within the dynamodb table as separate rows within the table.
// Use the [Graph] client to perform CRUD operations on stored entities.
type Entity interface {
	// DynamoID returns the unique identifier associated with the entity.
	DynamoID() string
	// DynamoEntityType returns the type of entity. This is used to
	// partition the collection of similar entities in the table's
	// global secondary index.
	DynamoEntityType() string
	// DynamoRelationships returns a mapping of this entity's relationships
	// to other [Entity] types. The key is the relationship name, and the value is
	// the relationship target's entity type.
	DynamoRelationships() map[string]string
	// DynamoIsReverseRelationship returns true if the relationship is stored
	// in reverse order. This is used to optimize reverse lookups.
	DynamoIsReverseRelationship(relation string) bool
	// DynamoGetRelationship returns a list of entities that are related to
	// this [Entity] by the given relationship.
	DynamoGetRelationship(relation string) []Entity
	// DynamoSetRelationship is called when a relationship between this [Entity] and the
	// target [Entity] is defined.
	DynamoSetRelationship(relation string, entityID string)
	// DynamoSetTimestamp provides the creation date and the last modification
	// date for this entity. [Entity] implementations should use the provided
	// values to update timestamp information if applicable.
	DynamoSetTimestamp(created, updated time.Time)
}

type nodeRef struct {
	Relationship     string `dynamodbav:"relationship"`
	SourceNodeID     string `dynamodbav:"sourceNodeID"`
	SourceNodePrefix string `dynamodbav:"sourceNodePrefix"`
	SourceNodeType   string `dynamodbav:"sourceNodeType"`
	TargetNodeID     string `dynamodbav:"targetNodeID"`
	TargetNodePrefix string `dynamodbav:"targetNodePrefix"`
	TargetNodeType   string `dynamodbav:"targetNodeType"`
}

func newNodeRef(source, target Entity, relation string, reverse bool) nodeRef {
	src, tgt := source, target
	if reverse {
		// relationships are stored in reverse order
		// so the source partition remains reasonably bounded.
		// these items can be retrieved together with the source
		// node on a reverse lookup query.
		src, tgt = target, source
	}
	return nodeRef{
		Relationship:     relation,
		SourceNodeID:     src.DynamoID(),
		SourceNodePrefix: src.DynamoEntityType(),
		SourceNodeType:   src.DynamoEntityType(),
		TargetNodeID:     tgt.DynamoID(),
		TargetNodePrefix: tgt.DynamoEntityType(),
		TargetNodeType:   tgt.DynamoEntityType(),
	}
}

func (n nodeRef) DynamoItemType() string { return n.Relationship }

func (n nodeRef) DynamoMarshalRecord(options *MarshalOptions) {
	options.HashKeyID = n.SourceNodeID
	options.SortKeyID = n.TargetNodeID
	options.HashKeyPrefix = n.SourceNodePrefix
	options.SortKeyPrefix = n.TargetNodePrefix
	options.SupportReverseLookup = true
	options.SupportCollectionQuery = false
}

type edge[T Entity] struct {
	Node T `dynamodbav:"node"`
}

func (e edge[T]) DynamoItemType() string { return e.Node.DynamoEntityType() }

func (e edge[T]) DynamoMarshalRecord(options *MarshalOptions) {
	options.HashKeyID = e.Node.DynamoID()
	options.SortKeyID = e.Node.DynamoID()
	options.HashKeyPrefix = e.Node.DynamoEntityType()
	options.SortKeyPrefix = e.Node.DynamoEntityType()
	options.SupportReverseLookup = true
	options.SupportCollectionQuery = true
}

// Graph is an abstraction layer over the base dynamodb client,
// treating the target table as a graph-based data structure.
// The table itself is used as an 2D adjacency matrix of
// nodes:
//
//	    A  B  C  D
//	  ------------
//	A | 1  1  0  0
//	B | 1  1  0  0
//	C | 0  0  1  0
//	D | 0  0  0  1
//
// Each cell in the matrix represents a directed edge from source
// nodes rows and target row columns. In the dynamodb table,
// table items represent edges to nodes within the graph. An edge
// is uniquely identified by its composite key; the hash key refers
// to the source node id while the sort key refers to the target node id.
//
// Nodes are represented by table partition; in this way, a node
// can be seen as the composition of its edges. Information about the node
// is stored in the "identity edge", or dynamodb item with an equivalent
// hash and sort key.
//
// Edge direction is represented by the partition where the item
// is stored; Given nodes A and B, edge A -> B is stored on partition A
// and is queried on node A. In order to query the reverse edge B -> A,
// Graph uses the reverse lookup index on node B.
type Graph[T Entity] struct {
	nodes Table[edge[T]]
	refs  Table[nodeRef]
}

func New[T Entity](tableName string, opts ...func(*Options)) Graph[T] {
	return Graph[T]{
		nodes: NewTable[edge[T]](tableName, opts...),
		refs:  NewTable[nodeRef](tableName, opts...),
	}
}

func (g Graph[T]) refsOf(node T) []nodeRef {
	relationships := node.DynamoRelationships()
	items := make([]nodeRef, 0, len(relationships))

	for relationship := range relationships {
		reverse := node.DynamoIsReverseRelationship(relationship)
		for _, ref := range node.DynamoGetRelationship(relationship) {
			record := newNodeRef(node, ref, relationship, reverse)
			items = append(items, record)
		}
	}
	return items
}

func (g Graph[T]) PutNode(node T, opts ...func(*Options)) operation.Put {
	return g.nodes.Put(edge[T]{Node: node}, opts...)
}

func (g Graph[T]) PutEdges(node T, opts ...func(*Options)) operation.BatchWriteItemCollection {
	batches := make(operation.BatchWriteItemCollection, 0)
	for _, ref := range g.refsOf(node) {
		batches = append(batches, g.refs.Put(ref))
	}
	return batches
}

func (g Graph[T]) GetNode(node T, opts ...func(*Options)) operation.Get {
	return g.nodes.Get(edge[T]{Node: node}, opts...)
}

func (g Graph[T]) UpdateNode(node T, strategy UpdateStrategy, opts ...func(*Options)) operation.UpdateItem {
	return g.nodes.Update(edge[T]{Node: node}, strategy, opts...)
}

type NodeAttribute string

func (a NodeAttribute) FilterAttribute() filter.Attribute {
	return filter.AttributeOf("data", "node", string(a))
}

func (a NodeAttribute) ExpressionName() expression.NameBuilder {
	name := strings.Join([]string{"data", "node", string(a)}, ".")
	return expression.Name(name)
}

func (g Graph[T]) DeleteNode(node T, opts ...func(*Options)) operation.Delete {
	return g.nodes.Delete(edge[T]{Node: node}, opts...)
}

func (g Graph[T]) DeleteNodeEdges(node T, relation string, opts ...func(*Options)) operation.BatchWriteItemCollection {
	g.refs.options.apply(opts)
	var (
		reverse    = node.DynamoIsReverseRelationship(relation)
		refs       = node.DynamoGetRelationship(relation)
		collection = make(operation.BatchWriteItemCollection, 0, len(refs))
	)

	for _, ref := range refs {
		collection = append(collection, g.refs.Delete(newNodeRef(node, ref, relation, reverse)))
	}

	return collection
}

func (g Graph[T]) DeleteAllNodeEdges(node T, opts ...func(*Options)) operation.BatchWriteItemCollection {
	var (
		relationships = node.DynamoRelationships()
		collection    = make(operation.BatchWriteItemCollection, 0)
	)

	for relationship := range relationships {
		collection = append(collection, g.DeleteNodeEdges(node, relationship)...)
	}

	return collection
}

func (g Graph[T]) UnmarshalNode(item ezddb.Item, opts ...func(*Options)) (T, error) {
	g.nodes.options.apply(opts)
	record, err := g.nodes.Unmarshal(item)
	var node T
	if err != nil {
		return node, err
	}
	node = record.Data.Node
	node.DynamoSetTimestamp(record.CreatedAt, record.UpdatedAt)
	return node, nil
}

func (g Graph[T]) UnmarshalNodeList(items []ezddb.Item, opts ...func(*Options)) ([]T, error) {
	g.nodes.options.apply(opts)
	result := make([]T, 0, len(items))
	for _, item := range items {
		node, err := g.UnmarshalNode(item)
		if err != nil {
			return nil, err
		}
		result = append(result, node)
	}
	return result, nil
}

func (g Graph[T]) UnmarshalEdgeList(node T, items []ezddb.Item, opts ...func(*Options)) error {
	g.refs.options.apply(opts)
	for _, item := range items {
		err := g.UnmarshalEdge(node, item)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g Graph[T]) UnmarshalEdge(node T, item ezddb.Item, opts ...func(*Options)) error {
	g.refs.options.apply(opts)
	record, err := g.refs.Unmarshal(item)
	if err != nil {
		return err
	}

	var (
		relationship = record.Data.Relationship
		reverse      = node.DynamoIsReverseRelationship(relationship)
	)

	if reverse {
		node.DynamoSetRelationship(relationship, record.Data.SourceNodeID)
	} else {
		node.DynamoSetRelationship(relationship, record.Data.TargetNodeID)
	}

	return nil
}

type ListNodesQueryBuilder[T Entity] struct {
	node   T
	graph  Graph[T]
	cursor string
	filter expression.ConditionBuilder
	limit  int
}

func (b ListNodesQueryBuilder[T]) Query(opts ...func(*Options)) operation.Query {
	return b.graph.nodes.Query(CollectionQuery{
		ItemType: b.node.DynamoEntityType(),
		Cursor:   b.cursor,
		Filter:   b.filter,
		Limit:    b.limit,
	}, opts...)
}

func (b *ListNodesQueryBuilder[T]) WithLimit(limit int) *ListNodesQueryBuilder[T] {
	b.limit = limit
	return b
}

func (b *ListNodesQueryBuilder[T]) WithCursor(cursor string, provider ezddb.StartKeyProvider) *ListNodesQueryBuilder[T] {
	b.cursor = cursor
	return b
}

func (b *ListNodesQueryBuilder[T]) WithFilter(filter expression.ConditionBuilder) *ListNodesQueryBuilder[T] {
	b.filter = filter
	return b
}

func (g Graph[T]) ListNodes(node T, relation string) ListNodesQueryBuilder[T] {
	return ListNodesQueryBuilder[T]{node: node, graph: g}
}

type ListEdgesQueryBuilder[T Entity] struct {
	node     T
	relation string
	graph    Graph[T]
	cursor   string
	filter   expression.ConditionBuilder
	limit    int
}

func (g Graph[T]) ListNodeEdges(node T, relation string) ListEdgesQueryBuilder[T] {
	return ListEdgesQueryBuilder[T]{node: node, relation: relation, graph: g}
}

func (b ListEdgesQueryBuilder[T]) Query(opts ...func(*Options)) operation.Query {
	b.graph.nodes.options.apply(opts)
	b.graph.refs.options.apply(opts)

	var (
		relationships     = b.node.DynamoRelationships()
		relatedEntityType = relationships[b.relation]
		marshalOptions    = b.graph.nodes.options.MarshalOptions
		record            = Marshal(edge[T]{Node: b.node}, marshalOptions...)
	)

	// perform a reverse lookup for the edges forming the target
	// relationship.
	if b.node.DynamoIsReverseRelationship(b.relation) {
		return b.graph.refs.Query(ReverseLookupQuery{
			SortKeyValue:      record.SK,
			GSI1SortKeyPrefix: relatedEntityType,
			Cursor:            b.cursor,
			Limit:             b.limit,
		})
	}

	// perform a partition lookup for the edges forming the source
	// relationship.
	return b.graph.refs.Query(LookupQuery{
		PartitionKeyValue: record.HK,
		SortKeyPrefix:     relatedEntityType,
		Cursor:            b.cursor,
		Limit:             b.limit,
	})
}

func (b *ListEdgesQueryBuilder[T]) WithLimit(limit int) *ListEdgesQueryBuilder[T] {
	b.limit = limit
	return b
}

func (b *ListEdgesQueryBuilder[T]) WithCursor(cursor string, provider ezddb.StartKeyProvider) *ListEdgesQueryBuilder[T] {
	b.cursor = cursor
	return b
}

func (b *ListEdgesQueryBuilder[T]) WithFilter(filter expression.ConditionBuilder) *ListEdgesQueryBuilder[T] {
	b.filter = filter
	return b
}
