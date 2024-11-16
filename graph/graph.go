package graph

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/operation"
)

// Vertex data can form relationships with other vertices
// in a [Graph2] via directed edges. Vertices should be uniquely
// identified by both [Vertex.DynamoItemType] and [Vertex.DynamoID].
type Vertex interface {
	Entity
	// DynamoID returns the unique identifier for the vertex.
	// It should be unique within the [Vertex.DynamoItemType] type.
	// For example, if the vertex type is "customer", the DynamoID
	// could be the customer's email address.
	DynamoID() string
}

// Edge connects two [Vertex] nodes together in a single direction,
// storing the start and end vertex ids. Edges between vertices can
// be grouped together by relationship. An edge with the same start
// and end vertex is considered an identity edge,
// storing the target vertex information.
type Edge struct {
	Vertex
	StartVertexID   string // The id of the starting vertex.
	EndVertexID     string // The id of the ending vertex.
	Relationship    string // The relationship between the two vertices.
	startVertexType string // The starting vertex type.
	endVertexType   string // The ending vertex type.
	isNode          bool   // true if the edge is an identity edge, or vertex node.
	// The edge item type, which is equivalent to the vertex type
	// for identity edges, or a constant value labeled "edge" otherwise.
	itemType string
}

func newVertex(v Vertex) Edge {
	return Edge{
		Vertex:        v,
		StartVertexID: v.DynamoID(),
		EndVertexID:   v.DynamoID(),
		itemType:      v.DynamoItemType(),
		isNode:        true,
	}
}

func newEdge(start, end Vertex, relationship string) Edge {
	return Edge{
		StartVertexID:   start.DynamoID(),
		startVertexType: start.DynamoItemType(),
		EndVertexID:     end.DynamoID(),
		endVertexType:   end.DynamoItemType(),
		itemType:        "edge",
		Relationship:    relationship,
	}
}

// UnmarshalVertex extracts the [Vertex] information from the specified
// item attribute map.
func UnmarshalVertex[V Vertex](item ezddb.Item) (V, error) {
	var (
		vertex V
		record = Record[Edge]{}
		err    = attributevalue.UnmarshalMap(item, &record)
	)

	if err == nil {
		vertex = record.Data.Vertex.(V)
	}

	return vertex, err
}

// EdgeUnmarshaler can extract edge information from dynamodb items,
// including the relationship name, starting vertex id, and ending
// vertex id.
type EdgeUnmarshaler interface {
	// UnmarshalEdge extracts the edge information from the specified
	// item attribute map.
	//
	// The relationship name, start vertex id, and end vertex id are
	// extracted from the item and passed to the method.
	//
	// The method should return an error if the item does not contain
	// the expected edge information.
	UnmarshalEdge(relationship string, startID, endID string) error
}

// UnmarshalEdge extracts the edge information from the specified
// item attribute map.
//
// The relationship name, start vertex id, and end vertex id are
// extracted from the item and passed to the [EdgeUnmarshaler].
func UnmarshalEdge[V EdgeUnmarshaler](item ezddb.Item, out EdgeUnmarshaler) error {
	var (
		record = Record[Edge]{}
		err    = attributevalue.UnmarshalMap(item, &record)
	)

	if err == nil {
		err = out.UnmarshalEdge(
			record.Data.Relationship,
			record.Data.StartVertexID,
			record.Data.EndVertexID,
		)
	}

	return err
}

func (e Edge) DynamoItemType() string { return e.itemType }

func (e Edge) DynamoMarshalRecord(opts *MarshalOptions) {
	// edges are stored on the end vertex; they can be queried
	// together with the node via reverse lookup.
	opts.HashKeyID = e.EndVertexID
	opts.HashKeyPrefix = e.endVertexType
	opts.SortKeyID = e.StartVertexID
	opts.SortKeyPrefix = e.startVertexType
	opts.SupportCollectionQuery = e.isNode
	opts.SupportReverseLookup = true

	if e.Relationship == "" {
		// ex, "customer/orders"
		opts.ReverseLookupSortKey = e.startVertexType + "/" + e.Relationship
	} else {
		// ex, "customer"
		opts.ReverseLookupSortKey = e.startVertexType
	}
}

type Graph2 struct {
	Table[Edge]
}

func New2(tableName string, opts ...func(*Options)) Graph2 {
	return Graph2{
		Table: NewTable[Edge](tableName, opts...),
	}
}

func (g Graph2) AddVertex(v Vertex, opts ...func(*Options)) operation.Put {
	g.options.apply(opts)
	return g.Put(newVertex(v))
}

func (g Graph2) AddEdge(start, end Vertex, relationship string, opts ...func(*Options)) operation.Put {
	g.options.apply(opts)
	return g.Put(newEdge(start, end, relationship))
}

func (g Graph2) GetVertex(v Vertex, opts ...func(*Options)) operation.Get {
	g.options.apply(opts)
	return g.Get(newVertex(v))
}

func (g Graph2) GetEdge(start, end Vertex, opts ...func(*Options)) operation.Get {
	g.options.apply(opts)
	return g.Get(newEdge(start, end, ""))
}

func (g Graph2) DeleteVertex(v Vertex, opts ...func(*Options)) operation.Delete {
	g.options.apply(opts)
	return g.Delete(newVertex(v))
}

func (g Graph2) DeleteEdge(start, end Vertex, opts ...func(*Options)) operation.Delete {
	g.options.apply(opts)
	return g.Delete(newEdge(start, end, ""))
}

type ListVerticesQuery struct {
	CollectionQuery
	Options []func(*Options)
}

func (g Graph2) ListVertices(v Vertex, opts ...func(ListVerticesQuery)) operation.Query {
	query := ListVerticesQuery{}
	query.ItemType = v.DynamoItemType()
	for _, o := range opts {
		o(query)
	}
	return g.Query(query.CollectionQuery, query.Options...)
}

type ListEdgesQuery struct {
	ReverseLookupQuery
	Options []func(*Options)
}

func (g Graph2) ListEdges(v Vertex, relationship string, opts ...func(ListEdgesQuery)) operation.Query {
	var (
		node  = Marshal(newVertex(v))
		edge  = Marshal(newEdge(v, v, relationship))
		query = ListEdgesQuery{}
	)

	query.SortKeyValue = node.SK
	if relationship != "" {
		query.GSI1SortKeyPrefix = edge.GSI1SK
	}

	for _, o := range opts {
		o(query)
	}

	return g.Query(query.ReverseLookupQuery, query.Options...)
}
