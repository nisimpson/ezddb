package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/filter"
	"github.com/nisimpson/ezddb/operation"
)

const (
	DefaultDelimiter = ":"

	AttributeNamePK                   = "pk"
	AttributeNameSK                   = "sk"
	AttributeNameItemType             = "itemType"
	AttributeNameCollectionSortKey    = "gsi2sk"
	AttributeNameReverseLookupSortKey = "gsi1sk"
	AttributeNameData                 = "data"
)

type Data interface {
	DynamoItemType() string
	DynamoMarshalRecord(*MarshalOptions)
}

type Record[T Data] struct {
	PK       string    `dynamodbav:"pk"`
	SK       string    `dynamodbav:"sk"`
	ItemType string    `dynamodbav:"itemType"`
	GSI1SK   string    `dynamodbav:"gsi1sk,omitempty"`
	GSI2SK   string    `dynamodbav:"gsi2sk,omitempty"`
	Expires  time.Time `dynamodbav:"expires,unixtime"`
	Data     *T        `dynamodbav:"data"`
}

func (r Record[T]) Key() ezddb.Item {
	return ezddb.Item{
		AttributeNamePK: &types.AttributeValueMemberS{Value: r.PK},
		AttributeNameSK: &types.AttributeValueMemberS{Value: r.SK},
	}
}

type clock func() time.Time

type MarshalOptions struct {
	HashID                 string
	SortID                 string
	HashPrefix             string
	SortPrefix             string
	Delimiter              string
	CollectionQuerySortKey string
	AllowReverseLookup     bool
	Tick                   clock
	ExpirationDate         time.Time
}

func Marshal[T Data](data T, opts ...func(*MarshalOptions)) Record[T] {
	options := MarshalOptions{
		Tick: time.Now,
	}

	// marshal data
	data.DynamoMarshalRecord(&options)

	// apply additional marshal options to override behavior
	for _, opt := range opts {
		opt(&options)
	}

	record := Record[T]{
		PK:       options.HashPrefix + DefaultDelimiter + options.HashID,
		SK:       options.SortPrefix + DefaultDelimiter + options.SortID,
		ItemType: data.DynamoItemType(),
		Data:     &data,
		Expires:  options.ExpirationDate,
		GSI2SK:   options.CollectionQuerySortKey,
	}

	if options.AllowReverseLookup {
		record.GSI1SK = record.PK
	}

	return record
}

type Options struct {
	TableName                string
	ReverseLookupIndexName   string
	CollectionQueryIndexName string
	MarshalMap               ezddb.ItemMarshaler
	UnmarshalMap             ezddb.ItemUnmarshaler
	MarshalOptions           []func(*MarshalOptions)
	PageCursorProvider       ezddb.StartKeyTokenProvider
}

func (o *Options) apply(opts []func(*Options)) {
	for _, opt := range opts {
		opt(o)
	}
}

type Table[T Data] struct {
	options Options
}

func New[T Data](tableName string, opts ...func(*Options)) *Table[T] {
	options := Options{
		TableName:                tableName,
		ReverseLookupIndexName:   "reverse-lookup-index",
		CollectionQueryIndexName: "collection-query-index",
	}

	options.apply(opts)
	return newTable[T](options)
}

func newTable[T Data](options Options) *Table[T] {
	return &Table[T]{options: options}
}

func (t Table[T]) PutFunc(ctx context.Context, data T, opts ...func(*Options)) operation.PutOperation {
	t.options.apply(opts)
	record := Marshal(data, t.options.MarshalOptions...)
	item, err := t.options.MarshalMap(record)
	if err != nil {
		err = fmt.Errorf("put func: marshal: %w", err)
	}

	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		return &dynamodb.PutItemInput{
			TableName: &t.options.TableName,
			Item:      item,
		}, err
	}
}

func (t Table[T]) GetFunc(ctx context.Context, data T, opts ...func(*Options)) operation.GetOperation {
	t.options.apply(opts)
	record := Marshal(data, t.options.MarshalOptions...)
	key := record.Key()
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		return &dynamodb.GetItemInput{
			TableName: &t.options.TableName,
			Key:       key,
		}, nil
	}
}

func (t Table[T]) DeleteFunc(ctx context.Context, data T, opts ...func(*Options)) operation.DeleteOperation {
	t.options.apply(opts)
	record := Marshal(data, t.options.MarshalOptions...)
	key := record.Key()
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		return &dynamodb.DeleteItemInput{
			TableName: &t.options.TableName,
			Key:       key,
		}, nil
	}
}

type QueryCriteria interface {
	indexName(Options) string
	expression(builder expression.Builder) (expression.Expression, error)
}

func (t Table[T]) QueryFunc(ctx context.Context, criteria QueryCriteria, opts ...func(*Options)) operation.QueryOperation {
	t.options.apply(opts)
	expr, err := criteria.expression(expression.NewBuilder())
	if err != nil {
		err = fmt.Errorf("query func: expression: %w", err)
	}

	indexName := criteria.indexName(t.options)
	return func(ctx context.Context) (*dynamodb.QueryInput, error) {
		input := &dynamodb.QueryInput{
			TableName:                 &t.options.TableName,
			KeyConditionExpression:    expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			IndexName:                 &indexName,
		}
		if indexName == "" {
			input.IndexName = nil
		}
		return input, err
	}
}

type QueryCollectionCriteria struct {
	Cursor        string
	Filter        filter.Expression
	CreatedBefore time.Time
	CreatedAfter  time.Time
	itemType      string
}

func (t Table[T]) QueryCollection(opts ...func(*QueryCollectionCriteria)) QueryCollectionCriteria {
	var data T
	criteria := QueryCollectionCriteria{
		itemType: data.DynamoItemType(),
	}
	for _, opt := range opts {
		opt(&criteria)
	}
	return criteria
}

type QueryPartition struct {
	Cursor        string
	Filter        filter.Expression
	SortKeyPrefix string
	ReverseLookup bool
	pk            string
	sk            string
}

func (q QueryPartition) indexName(o Options) string {
	if q.ReverseLookup {
		return o.ReverseLookupIndexName
	}
	return ""
}

func (q QueryPartition) expression(builder expression.Builder) (expression.Expression, error) {
	expr := sortKeyStartsWith(q.SortKeyPrefix)
	if q.Filter != nil {
		builder = builder.WithCondition(filter.Condition(q.Filter))
	}
	return builder.Build()
}

func (l QueryCollectionCriteria) indexName(o Options) string {
	return o.CollectionQueryIndexName
}

func (l QueryCollectionCriteria) expression(builder expression.Builder) (expression.Expression, error) {
	expr := itemTypeEquals(l.itemType)
	if !(l.CreatedBefore.IsZero() || l.CreatedAfter.IsZero()) {
		expr = expr.And(collectionSortKeyBetweenDates(l.CreatedAfter, l.CreatedBefore))
	}
	builder = builder.WithKeyCondition(expr)
	if l.Filter != nil {
		builder = builder.WithCondition(filter.Condition(l.Filter))
	}
	return builder.Build()
}

func sortKeyStartsWith(prefix string) expression.KeyConditionBuilder {
	return expression.KeyBeginsWith(expression.Key(AttributeNameSK), prefix)
}

func reverseLookupSortKeyStartsWith(prefix string) expression.KeyConditionBuilder {
	return expression.KeyBeginsWith(expression.Key(AttributeNameReverseLookupSortKey), prefix)
}

func itemTypeEquals(value string) expression.KeyConditionBuilder {
	return expression.KeyEqual(expression.Key(AttributeNameItemType), expression.Value(value))
}

func collectionSortKeyBetweenDates(start, end time.Time) expression.KeyConditionBuilder {
	return expression.KeyBetween(
		expression.Key(AttributeNameCollectionSortKey),
		expression.Value(start.Format(time.RFC3339Nano)),
		expression.Value(end.Format(time.RFC3339Nano)))
}
