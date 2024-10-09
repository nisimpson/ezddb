package graph

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/filter"
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
		HashPrefix: tgt.DynamoPrefix(),
		SortID:     src.DynamoID(),
		SortPrefix: src.DynamoPrefix(),
		Relation:   relation,
	}, func(io *EdgeOptions) {
		io.ItemType = fmt.Sprintf("ref:%s", relation)
		io.SupportsCollectionIndex = false
		io.SupportsReverseLookupIndex = true
	})
}

func (e Edge[T]) Key() ezddb.Item {
	return ezddb.Item{
		AttributePartitionKey: &types.AttributeValueMemberS{Value: e.PK},
		AttributeSortKey:      &types.AttributeValueMemberS{Value: e.SK},
	}
}

func (e Edge[T]) refs() []Edge[*nodeRef] {
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

type filters[T any] struct{}

func Filters[T Node](item T) filters[T] {
	return filters[T]{}
}

func (f filters[T]) RelationEquals(relation string) filter.Expression {
	attr := filter.AttributeOf[T](AttributeItemType)
	return filter.Equals[T](attr, fmt.Sprintf("ref:%s", relation))
}
