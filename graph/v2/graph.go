package graph

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/nisimpson/ezddb/operation"
)

type Node interface {
	DynamoNodeID() string
	DynamoNodePrefix() string
	DynamoNodeType() string
	DynamoNodeRelationships() map[string]Node
	DynamoNodeRefIsReverseLookup(relation string) bool
	DynamoNodeRef(relation string) []Node
	SetDynamoNodeRef(relation string, refID string)
	SetDynamoNodeTimestamp(created, updated time.Time)
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

func newNodeRef(source, target Node, relation string, reverse bool) nodeRef {
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
		SourceNodeID:     src.DynamoNodeID(),
		SourceNodePrefix: src.DynamoNodePrefix(),
		SourceNodeType:   src.DynamoNodeType(),
		TargetNodeID:     tgt.DynamoNodeID(),
		TargetNodePrefix: tgt.DynamoNodePrefix(),
		TargetNodeType:   tgt.DynamoNodeType(),
	}
}

func (n nodeRef) DynamoItemType() string { return n.Relationship }

func (n nodeRef) DynamoMarshalRecord(options *MarshalOptions) {
	options.HashID = n.SourceNodeID
	options.SortID = n.TargetNodeID
	options.HashPrefix = n.SourceNodePrefix
	options.SortPrefix = n.TargetNodePrefix
	options.SupportReverseLookup = true
	options.SupportCollectionQuery = false
}

type edge[T Node] struct {
	Node T `dynamodbav:"node"`
}

func (e edge[T]) DynamoItemType() string { return e.Node.DynamoNodeType() }

func (e edge[T]) DynamoMarshalRecord(options *MarshalOptions) {
	options.HashID = e.Node.DynamoNodeID()
	options.SortID = e.Node.DynamoNodeID()
	options.HashPrefix = e.Node.DynamoNodePrefix()
	options.SortPrefix = e.Node.DynamoNodePrefix()
	options.SupportReverseLookup = true
	options.SupportCollectionQuery = true
}

type Graph[T Node] struct {
	nodes Table[edge[T]]
	refs  Table[nodeRef]
}

func New[T Node](tableName string, opts ...func(*Options)) Graph[T] {
	return Graph[T]{
		nodes: NewTable[edge[T]](tableName, opts...),
		refs:  NewTable[nodeRef](tableName, opts...),
	}
}

func (g Graph[T]) refsOf(node T) []nodeRef {
	refs := node.DynamoNodeRelationships()
	items := make([]nodeRef, 0, len(refs))

	for relation := range refs {
		reverse := node.DynamoNodeRefIsReverseLookup(relation)
		for _, ref := range node.DynamoNodeRef(relation) {
			record := newNodeRef(node, ref, relation, reverse)
			items = append(items, record)
		}
	}
	return items
}

func (g Graph[T]) PutsNode(node T, opts ...func(*Options)) operation.PutOperation {
	return g.nodes.Puts(edge[T]{Node: node}, opts...)
}

func (g Graph[T]) PutsEdges(node T, opts ...func(*Options)) operation.BatchWriteCollection {
	batches := make(operation.BatchWriteCollection, 0)
	for _, ref := range g.refsOf(node) {
		batches = append(batches, g.refs.Puts(ref))
	}
	return batches
}

func (g Graph[T]) GetsNode(node T, opts ...func(*Options)) operation.GetOperation {
	return g.nodes.Gets(edge[T]{Node: node}, opts...)
}

type ListNodesQueryBuilder[T Node] struct {
	node   T
	graph  Graph[T]
	cursor string
	filter expression.ConditionBuilder
	limit  int
}

func (b ListNodesQueryBuilder[T]) BuildQuery(opts ...func(*Options)) operation.QueryOperation {
	return b.graph.nodes.Queries(CollectionQuery{
		ItemType: b.node.DynamoNodeType(),
		Cursor:   b.cursor,
		Filter:   b.filter,
		Limit:    b.limit,
	}, opts...)
}

func (b *ListNodesQueryBuilder[T]) WithLimit(limit int) *ListNodesQueryBuilder[T] {
	b.limit = limit
	return b
}

func (b *ListNodesQueryBuilder[T]) WithCursor(cursor string) *ListNodesQueryBuilder[T] {
	b.cursor = cursor
	return b
}

func (b *ListNodesQueryBuilder[T]) WithFilter(filter expression.ConditionBuilder) *ListNodesQueryBuilder[T] {
	b.filter = filter
	return b
}

func (g Graph[T]) ListNodesQueryBuilder(node T, relation string) ListNodesQueryBuilder[T] {
	return ListNodesQueryBuilder[T]{node: node, graph: g}
}

type ListEdgesQueryBuilder[T Node] struct {
	node     T
	relation string
	graph    Graph[T]
	cursor   string
	filter   expression.ConditionBuilder
	limit    int
}

func (g Graph[T]) ListEdgesQueryBuilder(node T, relation string) ListEdgesQueryBuilder[T] {
	return ListEdgesQueryBuilder[T]{node: node, relation: relation, graph: g}
}

func (b ListEdgesQueryBuilder[T]) BuildQuery(opts ...func(*Options)) operation.QueryOperation {
	b.graph.nodes.options.apply(opts)
	b.graph.refs.options.apply(opts)

	var (
		definitions    = b.node.DynamoNodeRelationships()
		def            = definitions[b.relation]
		marshalOptions = b.graph.nodes.options.MarshalOptions
		record         = Marshal(edge[T]{Node: b.node}, marshalOptions...)
	)

	// perform a reverse lookup for the edges forming the target
	// relationship.
	if b.node.DynamoNodeRefIsReverseLookup(b.relation) {
		return b.graph.refs.Queries(ReverseLookupQuery{
			SortKeyValue:      record.SK,
			GSI1SortKeyPrefix: def.DynamoNodePrefix(),
			Cursor:            b.cursor,
			Limit:             b.limit,
		})
	}

	// perform a partition lookup for the edges forming the source
	// relationship.
	return b.graph.refs.Queries(LookupQuery{
		PartitionKeyValue: record.PK,
		SortKeyPrefix:     def.DynamoNodePrefix(),
		Cursor:            b.cursor,
		Limit:             b.limit,
	})
}

func (b *ListEdgesQueryBuilder[T]) WithLimit(limit int) *ListEdgesQueryBuilder[T] {
	b.limit = limit
	return b
}

func (b *ListEdgesQueryBuilder[T]) WithCursor(cursor string) *ListEdgesQueryBuilder[T] {
	b.cursor = cursor
	return b
}

func (b *ListEdgesQueryBuilder[T]) WithFilter(filter expression.ConditionBuilder) *ListEdgesQueryBuilder[T] {
	b.filter = filter
	return b
}
