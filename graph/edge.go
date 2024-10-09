package graph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

var (
	ErrNotFound = errors.New("not found")
)

const (
	AttributePartitionKey           = "pk"
	AttributeSortKey                = "sk"
	AttributeItemType               = "item_type"
	AttributeCollectionQuerySortKey = "gsi2_sk"
	AttributeReverseLookupSortKey   = "gsi1_sk"
	AttributeUpdatedAt              = "updated_at"
)

type Node interface {
	DynamoID() string
	DynamoPrefix() string
	DynamoItemType() string
	DynamoRelationships() map[string][]Node
	DynamoMarshal(createdAt, updatedAt time.Time) error
	DynamoUnmarshal(createdAt, updatedAt time.Time) error
	DynamoUnmarshalRef(relation string, refID string) error
}

type Edge[T Node] struct {
	PK        string    `dynamodbav:"pk"`
	SK        string    `dynamodbav:"sk"`
	ItemType  string    `dynamodbav:"item_type"`
	GSI1SK    string    `dynamodbav:"gsi1_sk"`
	GSI2SK    string    `dynamodbav:"gsi2_sk"`
	CreatedAt time.Time `dynamodbav:"created_at"`
	UpdatedAt time.Time `dynamodbav:"updated_at"`
	Expires   time.Time `dynamodbav:"expires,unixtime"`
	Data      T         `dynamodbav:"data"`
}

type Clock func() time.Time

type EdgeOptions struct {
	HashID                     string
	HashPrefix                 string
	SortID                     string
	SortPrefix                 string
	ItemType                   string
	SupportsReverseLookupIndex bool
	SupportsCollectionIndex    bool
	Tick                       Clock
	ExpirationDate             time.Time
}

func NewEdge[T Node](data T, opts ...func(*EdgeOptions)) Edge[T] {
	options := EdgeOptions{
		Tick:                       time.Now,
		HashID:                     data.DynamoID(),
		HashPrefix:                 data.DynamoPrefix(),
		SortID:                     data.DynamoID(),
		SortPrefix:                 data.DynamoPrefix(),
		ItemType:                   data.DynamoItemType(),
		SupportsReverseLookupIndex: true,
		SupportsCollectionIndex:    true,
	}

	for _, opt := range opts {
		opt(&options)
	}

	now := options.Tick().UTC()

	edge := Edge[T]{
		PK:        options.HashPrefix + ":" + options.HashID,
		SK:        options.SortPrefix + ":" + options.SortID,
		ItemType:  options.ItemType,
		CreatedAt: now,
		UpdatedAt: now,
		Expires:   options.ExpirationDate,
	}

	if options.SupportsCollectionIndex {
		edge.GSI2SK = edge.CreatedAt.Format(time.RFC3339)
	}

	if options.SupportsReverseLookupIndex {
		edge.GSI1SK = edge.PK
	}

	return edge
}

type nodeRef struct {
	HashID     string
	HashPrefix string
	SortID     string
	SortPrefix string
	Relation   string
}

func (n nodeRef) DynamoID() string                                   { return n.HashID }
func (n nodeRef) DynamoPrefix() string                               { return n.HashPrefix }
func (n nodeRef) DynamoItemType() string                             { return n.Relation }
func (nodeRef) DynamoRelationships() map[string][]Node               { return nil }
func (nodeRef) DynamoUnmarshal(createdAt, updatedAt time.Time) error { return nil }
func (nodeRef) DynamoUnmarshalRef(string, string) error              { return nil }
func (nodeRef) DynamoMarshal(createdAt, updatedAt time.Time) error   { return nil }

func newNodeRef(src, tgt Node, relation string) Edge[*nodeRef] {
	return NewEdge(&nodeRef{
		HashID:     tgt.DynamoID(),
		SortID:     src.DynamoID(),
		HashPrefix: tgt.DynamoPrefix(),
		SortPrefix: src.DynamoPrefix(),
		Relation:   relation,
	}, func(io *EdgeOptions) {
		io.ItemType = fmt.Sprintf("ref:%s", relation)
		io.SupportsCollectionIndex = false
		io.SupportsReverseLookupIndex = true
	})
}

func (e Edge[T]) Refs() []Edge[*nodeRef] {
	relationships := e.Data.DynamoRelationships()
	if len(relationships) == 0 {
		return nil
	}

	refs := make([]Edge[*nodeRef], 0, len(relationships))
	for name, nodes := range relationships {
		for _, node := range nodes {
			refs = append(refs, newNodeRef(e.Data, node, name))
		}
	}
	return refs
}

func (e Edge[T]) IsNode() bool {
	return e.PK == e.SK
}

func (e Edge[T]) Key() ezddb.Item {
	return ezddb.Item{
		AttributePartitionKey: &types.AttributeValueMemberS{Value: e.PK},
		AttributeSortKey:      &types.AttributeValueMemberS{Value: e.SK},
	}
}

func (e Edge[T]) marshal(m ezddb.ItemMarshaler) (ezddb.Item, error) {
	err1 := e.Data.DynamoMarshal(e.CreatedAt, e.UpdatedAt)
	item, err2 := m(e)
	return item, errors.Join(err1, err2)
}

func (e *Edge[T]) unmarshal(item ezddb.Item, u ezddb.ItemUnmarshaler) error {
	if err := u(item, e); err != nil {
		return err
	}
	return e.Data.DynamoUnmarshal(e.CreatedAt, e.UpdatedAt)
}

func UnmarshalCollection[T Node](ctx context.Context, items []ezddb.Item, opts ...OptionsFunc) ([]T, error) {
	options := Options{UnmarshalItem: attributevalue.UnmarshalMap}
	options.apply(opts)

	nodes := make([]T, 0, len(items))
	for _, item := range items {
		node := Edge[T]{}
		err := options.UnmarshalItem(item, &node)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal node: %w", err)
		}
		err = node.Data.DynamoUnmarshal(node.CreatedAt, node.UpdatedAt)
		nodes = append(nodes, node.Data)
	}

	return nodes, nil
}

func UnmarshalPartition[T Node](ctx context.Context, data T, items []ezddb.Item, opts ...OptionsFunc) (T, error) {
	options := Options{UnmarshalItem: attributevalue.UnmarshalMap}
	options.apply(opts)

	node := NewEdge(data)
	errs := make([]error, 0, len(items))
	for _, item := range items {
		itemType := item[AttributeItemType].(*types.AttributeValueMemberS).Value
		if itemType == node.ItemType {
			err := options.UnmarshalItem(item, node)
			errs = append(errs, err)
			continue
		}
		ref := Edge[nodeRef]{}
		err := options.UnmarshalItem(item, &ref)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		relation := ref.Data.Relation
		refID := ref.Data.HashID
		err = node.Data.DynamoUnmarshalRef(relation, refID)
		errs = append(errs, err)
	}
	return node.Data, errors.Join(errs...)
}

func isTypeOf[T Node](node T, item ezddb.Item) bool {
	if attr, ok := item[AttributeItemType].(*types.AttributeValueMemberS); !ok {
		return false
	} else {
		return attr.Value == node.DynamoItemType()
	}
}
