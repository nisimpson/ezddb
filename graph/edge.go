package graph

import (
	"errors"
	"fmt"
	"time"

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
	AttributeCreatedAt              = "created_at"
	AttributeExpires                = "expires"
	AttributeData                   = "data"
)

type Node interface {
	DynamoID() string
	DynamoPrefix() string
	DynamoItemType() string
	DynamoRefs() map[string]Node
	DynamoMarshal(createdAt, updatedAt time.Time) error
	DynamoMarshalRef(relation string) ([]Node, bool)
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

// nodeRef represents the relationship
// between the source node and target node. Using the directed graph model,
// the relation is "source -> target". The relation prefix is used to
// distinguish between different types of relationships. For example, a "follows"
// relation might represent a user following another user or a post being commented on by another user.
type nodeRef struct {
	SourceID     string
	SourcePrefix string
	TargetID     string
	TargetPrefix string
	Relation     string
	Reverse      bool
}

func (n nodeRef) DynamoID() string {
	if n.Reverse {
		return n.SourceID
	}
	return n.TargetID
}

func (n nodeRef) DynamoPrefix() string {
	if n.Reverse {
		return n.SourcePrefix
	}
	return n.TargetPrefix
}

func (n nodeRef) DynamoItemType() string                             { return n.Relation }
func (nodeRef) DynamoRefs() map[string]Node                          { return nil }
func (nodeRef) DynamoUnmarshal(createdAt, updatedAt time.Time) error { return nil }
func (nodeRef) DynamoUnmarshalRef(string, string) error              { return nil }
func (nodeRef) DynamoMarshal(createdAt, updatedAt time.Time) error   { return nil }
func (nodeRef) DynamoMarshalRef(relation string) ([]Node, bool)      { return nil, false }

// refItemType returns the item type for a reference edge.
// The item type is a combination of the relation and the source node's item type.
// This allows us to query for all references of a specific type.
// For example, given source node type "user" and relation "follows",
// the item type would be "follows:user"
func refItemType(src Node, relation string) string {
	return fmt.Sprintf("%s:%s", relation, src.DynamoItemType())
}

// newNodeRef creates a new reference edge that represents the relationship
// between the source node and target node.
func newNodeRef(src, tgt Node, relation string, reverse bool) Edge[*nodeRef] {
	return NewEdge(&nodeRef{
		TargetID:     tgt.DynamoID(),
		TargetPrefix: tgt.DynamoPrefix(),
		SourceID:     src.DynamoID(),
		SourcePrefix: src.DynamoPrefix(),
		Relation:     relation,
		Reverse:      reverse,
	}, func(io *EdgeOptions) {
		io.ItemType = refItemType(src, relation)
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
	relationships := e.Data.DynamoRefs()
	if len(relationships) == 0 {
		return nil
	}

	refs := make([]Edge[*nodeRef], 0, len(relationships))
	for name := range relationships {
		nodes, reverse := e.Data.DynamoMarshalRef(name)
		for _, node := range nodes {
			src, tgt := e.Data, node
			if reverse {
				refs = append(refs, newNodeRef(tgt, src, name, true))
				continue
			}
			refs = append(refs, newNodeRef(src, tgt, name, false))
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
