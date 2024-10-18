package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
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
	PK        string    `dynamodbav:"pk"`
	SK        string    `dynamodbav:"sk"`
	ItemType  string    `dynamodbav:"itemType"`
	GSI1SK    string    `dynamodbav:"gsi1sk,omitempty"`
	GSI2SK    string    `dynamodbav:"gsi2sk,omitempty"`
	CreatedAt time.Time `dynamodbav:"createdAt"`
	UpdatedAt time.Time `dynamodbav:"updatedAt"`
	Expires   time.Time `dynamodbav:"expires,unixtime"`
	Data      *T        `dynamodbav:"data"`
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
	SupportReverseLookup   bool
	SupportCollectionQuery bool
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

	ts := options.Tick().UTC()

	record := Record[T]{
		PK:        options.HashPrefix + DefaultDelimiter + options.HashID,
		SK:        options.SortPrefix + DefaultDelimiter + options.SortID,
		ItemType:  data.DynamoItemType(),
		Data:      &data,
		Expires:   options.ExpirationDate,
		CreatedAt: ts,
		UpdatedAt: ts,
	}

	if options.SupportReverseLookup {
		record.GSI1SK = record.PK
	}
	if options.SupportCollectionQuery {
		record.GSI2SK = ts.Format(time.RFC3339)
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
	Tick                     clock
}

func (o *Options) apply(opts []func(*Options)) {
	for _, opt := range opts {
		opt(o)
	}
}

type Table[T Data] struct {
	options Options
}

func NewTable[T Data](tableName string, opts ...func(*Options)) Table[T] {
	options := Options{
		TableName:                tableName,
		ReverseLookupIndexName:   "reverse-lookup-index",
		CollectionQueryIndexName: "collection-query-index",
		Tick:                     time.Now,
	}

	options.apply(opts)
	return newTable[T](options)
}

func newTable[T Data](options Options) Table[T] {
	return Table[T]{options: options}
}

func (t Table[T]) Puts(data T, opts ...func(*Options)) operation.PutOperation {
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

func (t Table[T]) Gets(data T, opts ...func(*Options)) operation.GetOperation {
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

func (t Table[T]) Deletes(data T, opts ...func(*Options)) operation.DeleteOperation {
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

type QueryStrategy interface {
	modify(op operation.QueryOperation, opts Options) operation.QueryOperation
}

func (t Table[T]) Queries(strategy QueryStrategy, opts ...func(*Options)) operation.QueryOperation {
	t.options.apply(opts)
	return strategy.modify(func(ctx context.Context) (*dynamodb.QueryInput, error) {
		return &dynamodb.QueryInput{
			TableName: &t.options.TableName,
		}, nil
	}, t.options)
}

type ReverseLookupQuery struct {
	SortKeyValue      string
	GSI1SortKeyPrefix string
	Filter            expression.ConditionBuilder
	Cursor            string
	Limit             int
}

func (q ReverseLookupQuery) modify(op operation.QueryOperation, opts Options) operation.QueryOperation {
	builder := expression.NewBuilder()
	keyCondition := sortKeyEquals(q.SortKeyValue)
	if q.GSI1SortKeyPrefix != "" {
		keyCondition = keyCondition.And(reverseLookupSortKeyStartsWith(q.GSI1SortKeyPrefix))
	}
	if q.Filter.IsSet() {
		builder = builder.WithCondition(q.Filter)
	}
	builder = builder.WithKeyCondition(keyCondition)
	return op.Modify(
		operation.QueryModifierFunc(func(ctx context.Context, qi *dynamodb.QueryInput) error {
			qi.IndexName = &opts.ReverseLookupIndexName
			return nil
		}),
		operation.WithExpressionBuilder(builder),
		operation.WithLastToken(q.Cursor, nil), // todo: add start key provider
		operation.WithLimit(q.Limit),
	)
}

type CollectionQuery struct {
	ItemType      string
	CreatedBefore time.Time
	CreatedAfter  time.Time
	Filter        expression.ConditionBuilder
	Cursor        string
	Limit         int
}

func (q CollectionQuery) modify(op operation.QueryOperation, opts Options) operation.QueryOperation {
	builder := expression.NewBuilder()

	// we consider the zero values for the lower and upper date bounds to be
	// the year 0001 and the current day, respectively.
	if q.CreatedBefore.IsZero() {
		q.CreatedBefore = opts.Tick().UTC()
	}
	// apply the key condition
	builder = builder.WithKeyCondition(
		itemTypeEquals(q.ItemType).And(collectionSortKeyBetweenDates(
			q.CreatedAfter,
			q.CreatedBefore,
		)))
	// apply the filter condition
	if q.Filter.IsSet() {
		builder = builder.WithCondition(q.Filter)
	}

	return op.Modify(
		operation.QueryModifierFunc(func(ctx context.Context, qi *dynamodb.QueryInput) error {
			qi.IndexName = &opts.CollectionQueryIndexName
			return nil
		}),
		operation.WithExpressionBuilder(builder),
		operation.WithLastToken(q.Cursor, nil), // todo add paginator
		operation.WithLimit(q.Limit),
	)
}

type LookupQuery struct {
	PartitionKeyValue string
	SortKeyPrefix     string
	Filter            expression.ConditionBuilder
	Cursor            string
	Limit             int
}

func (q LookupQuery) modify(op operation.QueryOperation, opts Options) operation.QueryOperation {
	builder := expression.NewBuilder()
	keyCondition := partitionKeyEquals(q.PartitionKeyValue)
	if q.SortKeyPrefix != "" {
		keyCondition = keyCondition.And(sortKeyStartsWith(q.SortKeyPrefix))
	}
	if q.Filter.IsSet() {
		builder = builder.WithCondition(q.Filter)
	}
	builder = builder.WithKeyCondition(keyCondition)
	return op.Modify(
		operation.WithExpressionBuilder(builder),
		operation.WithLastToken(q.Cursor, nil), // todo: add start key provider
		operation.WithLimit(q.Limit),
	)
}

func sortKeyStartsWith(prefix string) expression.KeyConditionBuilder {
	return expression.KeyBeginsWith(expression.Key(AttributeNameSK), prefix)
}

func partitionKeyEquals(value string) expression.KeyConditionBuilder {
	return expression.KeyEqual(expression.Key(AttributeNamePK), expression.Value(value))
}

func sortKeyEquals(value string) expression.KeyConditionBuilder {
	return expression.KeyEqual(expression.Key(AttributeNameSK), expression.Value(value))
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
