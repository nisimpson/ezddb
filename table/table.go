package table

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/filter"
	"github.com/nisimpson/ezddb/internal/proxy"
	"github.com/nisimpson/ezddb/operation"
)

const (
	DefaultDelimiter = ":"

	AttributeNameHK                   = "hk"
	AttributeNameSK                   = "sk"
	AttributeNameItemType             = "itemType"
	AttributeNameCollectionSortKey    = "gsi2Sk"
	AttributeNameReverseLookupSortKey = "gsi1Sk"
	AttributeNameData                 = "data"
	AttributeNameCreatedAt            = "createdAt"
	AttributeNameUpdatedAt            = "updatedAt"
	AttributeNameExpires              = "expires"
)

var (
	AttributeHK            = filter.AttributeOf(AttributeNameHK)
	AttributeSK            = filter.AttributeOf(AttributeNameSK)
	AttributeCreatedAt     = filter.AttributeOf(AttributeNameCreatedAt)
	AttributeUpdatedAt     = filter.AttributeOf(AttributeNameUpdatedAt)
	AttributeExpires       = filter.AttributeOf(AttributeNameExpires)
	AttributeItemType      = filter.AttributeOf(AttributeNameItemType)
	AttributeCollection    = filter.AttributeOf(AttributeNameCollectionSortKey)
	AttributeReverseLookup = filter.AttributeOf(AttributeNameReverseLookupSortKey)
)

type Record[T any] struct {
	HK        string    `dynamodbav:"hk"`
	SK        string    `dynamodbav:"sk"`
	ItemType  string    `dynamodbav:"itemType"`
	GSI1SK    string    `dynamodbav:"gsi1Sk,omitempty"`
	GSI2SK    string    `dynamodbav:"gsi2Sk,omitempty"`
	CreatedAt time.Time `dynamodbav:"createdAt"`
	UpdatedAt time.Time `dynamodbav:"updatedAt"`
	Expires   time.Time `dynamodbav:"expires,unixtime"`
	Data      T         `dynamodbav:"data"`
}

func (r Record[T]) Key() ezddb.Item {
	return ezddb.Item{
		AttributeNameHK: &types.AttributeValueMemberS{Value: r.HK},
		AttributeNameSK: &types.AttributeValueMemberS{Value: r.SK},
	}
}

type clock func() time.Time

type MarshalOptions struct {
	HashKeyID              string
	SortKeyID              string
	HashKeyPrefix          string
	SortKeyPrefix          string
	Delimiter              string
	ItemType               string
	SupportReverseLookup   bool
	SupportCollectionQuery bool
	ReverseLookupSortKey   string
	Tick                   clock
	ExpirationDate         time.Time
}

type RecordMarshaler interface {
	DynamoMarshalRecord(*MarshalOptions)
}

func MarshalRecord[T RecordMarshaler](data T, opts ...func(*MarshalOptions)) Record[T] {
	options := MarshalOptions{
		Tick: time.Now,
	}

	// marshal data
	data.DynamoMarshalRecord(&options)

	// Apply additional marshal options to override behavior
	for _, opt := range opts {
		opt(&options)
	}

	ts := options.Tick().UTC()

	record := Record[T]{
		HK:        options.HashKeyPrefix + DefaultDelimiter + options.HashKeyID,
		SK:        options.SortKeyPrefix + DefaultDelimiter + options.SortKeyID,
		ItemType:  options.ItemType,
		Data:      data,
		Expires:   options.ExpirationDate,
		CreatedAt: ts,
		UpdatedAt: ts,
	}

	if options.SupportReverseLookup {
		record.GSI1SK = options.ReverseLookupSortKey

		if record.GSI1SK == "" {
			record.GSI1SK = record.HK
		}
	}

	if options.SupportCollectionQuery {
		record.GSI2SK = ts.Format(time.RFC3339)
	}

	return record
}

type IDGenerator interface {
	GenerateID(context.Context) string
}

type Options struct {
	TableName                string
	ReverseLookupIndexName   string
	CollectionQueryIndexName string
	Marshaler                proxy.MapMarshaler
	MarshalOptions           []func(*MarshalOptions)
	Tick                     clock
	IDGenerator              IDGenerator
	EncodeDecode             proxy.GobEncoderDecoder
}

func (o *Options) Apply(opts []func(*Options)) {
	for _, opt := range opts {
		opt(o)
	}
}

type Table[T RecordMarshaler] struct {
	Options Options
}

func New[T RecordMarshaler](tableName string, opts ...func(*Options)) Table[T] {
	options := Options{
		TableName:                tableName,
		ReverseLookupIndexName:   "reverse-lookup-index",
		CollectionQueryIndexName: "collection-query-index",
		Tick:                     time.Now,
		EncodeDecode:             proxy.Default,
		Marshaler:                proxy.Default,
	}

	options.Apply(opts)
	return newTable[T](options)
}

func newTable[T RecordMarshaler](options Options) Table[T] {
	return Table[T]{Options: options}
}

type PaginationClient interface {
	ezddb.Getter
	ezddb.Putter
}

type Paginator struct {
	options Options
	client  PaginationClient
}

var (
	_ ezddb.StartKeyProvider      = Paginator{}
	_ ezddb.StartKeyTokenProvider = Paginator{}
)

type page struct {
	ID       string
	StartKey []byte
}

func (p page) DynamoMarshalRecord(o *MarshalOptions) {
	o.HashKeyID = p.ID
	o.SortKeyID = p.ID
	o.HashKeyPrefix = "pagination"
	o.SortKeyPrefix = "pagination"
	o.ItemType = "pagination"
	o.SupportCollectionQuery = false
	o.SupportReverseLookup = false
}

// GetStartKey implements ezddb.StartKeyProvider.
func (p Paginator) GetStartKey(ctx context.Context, token string) (ezddb.Item, error) {
	if token == "" {
		return nil, nil
	}

	var (
		table = newTable[page](p.options)
	)

	out, err := table.Get(page{ID: token}).Execute(ctx, p.client)
	panicOnErr(err, "get page record from token '%s'", token)

	if out.Item == nil {
		// no start key found
		return nil, nil
	}

	record, err := table.Unmarshal(out.Item)
	panicOnErr(err, "unmarshal page record from token '%s'", token)

	var (
		data = record.Data.StartKey
		item = ezddb.Item{}
	)

	err = json.Unmarshal(data, &item)
	panicOnErr(err, "unmarshal page token '%s'", token)

	return item, nil
}

// GetStartKeyToken implements ezddb.StartKeyTokenProvider.
func (p Paginator) GetStartKeyToken(ctx context.Context, startKey map[string]types.AttributeValue) (string, error) {
	if len(startKey) == 0 {
		return "", nil
	}

	data, err := json.Marshal(startKey)
	panicOnErr(err, "get start key: marshal")

	var (
		id    = p.options.IDGenerator.GenerateID(ctx)
		table = newTable[page](p.options)
	)

	_, err = table.Put(page{ID: id, StartKey: data}).Execute(ctx, p.client)
	return id, err
}

func (t Table[T]) Paginator(client PaginationClient) Paginator {
	return Paginator{client: client, options: t.Options}
}

func (t Table[T]) Put(data T, opts ...func(*Options)) operation.Put {
	t.Options.Apply(opts)
	record := MarshalRecord(data, t.Options.MarshalOptions...)
	item, err := t.Options.Marshaler.MarshalMap(record)
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		return &dynamodb.PutItemInput{
			TableName: &t.Options.TableName,
			Item:      item,
		}, err
	}
}

func (t Table[T]) Get(data T, opts ...func(*Options)) operation.Get {
	t.Options.Apply(opts)
	record := MarshalRecord(data, t.Options.MarshalOptions...)
	key := record.Key()
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		return &dynamodb.GetItemInput{
			TableName: &t.Options.TableName,
			Key:       key,
		}, nil
	}
}

func (t Table[T]) Delete(data T, opts ...func(*Options)) operation.Delete {
	t.Options.Apply(opts)
	record := MarshalRecord(data, t.Options.MarshalOptions...)
	key := record.Key()
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		return &dynamodb.DeleteItemInput{
			TableName: &t.Options.TableName,
			Key:       key,
		}, nil
	}
}

type UpdateStrategy interface {
	modify(op operation.UpdateItem, opts Options) operation.UpdateItem
}

func (t Table[T]) Update(id T, strategy UpdateStrategy, opts ...func(*Options)) operation.UpdateItem {
	t.Options.Apply(opts)
	record := MarshalRecord(id, t.Options.MarshalOptions...)

	return strategy.modify(func(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
		return &dynamodb.UpdateItemInput{
			TableName:    &t.Options.TableName,
			Key:          record.Key(),
			ReturnValues: types.ReturnValueAllNew,
		}, nil
	}, t.Options)
}

type UpdateAttributeFunc = func(update expression.UpdateBuilder) expression.UpdateBuilder

type UpdateDataAttributes struct {
	Attributes []string
	Updates    map[string]UpdateAttributeFunc
}

func (u UpdateDataAttributes) modify(op operation.UpdateItem, opts Options) operation.UpdateItem {
	var (
		builder = expression.NewBuilder()
		update  = updateTimestamp(opts.Tick().UTC())
	)

	for _, attr := range u.Attributes {
		fn, ok := u.Updates[attr]
		if !ok {
			continue
		}
		update = fn(update)
	}

	builder = builder.WithUpdate(update)
	return op.Modify(operation.WithExpressionBuilder(builder))
}

type QueryStrategy interface {
	modify(op operation.Query, opts Options) operation.Query
}

func (t Table[T]) Query(strategy QueryStrategy, opts ...func(*Options)) operation.Query {
	t.Options.Apply(opts)
	return strategy.modify(func(ctx context.Context) (*dynamodb.QueryInput, error) {
		return &dynamodb.QueryInput{
			TableName: &t.Options.TableName,
		}, nil
	}, t.Options)
}

func (t Table[T]) Unmarshal(item ezddb.Item, opts ...func(*Options)) (Record[T], error) {
	t.Options.Apply(opts)
	var record Record[T]
	err := t.Options.Marshaler.UnmarshalMap(item, &record)
	return record, err
}

type QueryOptions struct {
	// The target partition key value. The target attribute depends on the GSI or table
	// queried:
	//	table -> "hk"
	//	gsi1 (reverse-lookup) -> "sk"
	//	gsi2 (collection) ->  "itemtype"
	PartitionKeyValue string
	// The target sort key prefix, used to narrow down queries. The target attribute depends
	// on the GSI or table queried:
	//	table -> "sk"
	//	gsi1 (reverse-lookup) -> "gsi1Sk"
	// 	gsi2 (collection) -> "gsi2Sk"
	SortKeyPrefix string
	// The upper limit date filter, or latest creation date. Only used by collection index queries.
	CreatedBefore time.Time
	// The lower limit date filter, or earliest creation date. Only used by collection index queries.
	CreatedAfter time.Time

	Filter           expression.ConditionBuilder // A filter condition to Apply to the query.
	StartKeyProvider ezddb.StartKeyProvider      // The start key provider used for pagination.
	Cursor           string                      // The last pagination cursor token.
	Limit            int                         // The number of items to return.
}

type ReverseLookupQuery struct {
	QueryOptions
}

func (q ReverseLookupQuery) modify(op operation.Query, opts Options) operation.Query {
	builder := expression.NewBuilder()
	keyCondition := sortKeyEquals(q.PartitionKeyValue)
	if q.SortKeyPrefix != "" {
		keyCondition = keyCondition.And(reverseLookupSortKeyStartsWith(q.SortKeyPrefix))
	}
	if q.Filter.IsSet() {
		builder = builder.WithFilter(q.Filter)
	}
	builder = builder.WithKeyCondition(keyCondition)
	op = op.Modify(
		operation.QueryModifierFunc(func(ctx context.Context, qi *dynamodb.QueryInput) error {
			qi.IndexName = &opts.ReverseLookupIndexName
			return nil
		}),
		operation.WithExpressionBuilder(builder),
		operation.WithLimit(q.Limit),
	)
	if q.Cursor != "" {
		op = op.Modify(operation.WithLastToken(q.Cursor, q.StartKeyProvider))
	}
	return op
}

type CollectionQuery struct {
	QueryOptions
}

func (q CollectionQuery) modify(op operation.Query, opts Options) operation.Query {
	builder := expression.NewBuilder()

	// we consider the zero values for the lower and upper date bounds to be
	// the year 0001 and the current day, respectively.
	if q.CreatedBefore.IsZero() {
		q.CreatedBefore = opts.Tick().UTC().Add(1 * time.Hour)
	}
	// Apply the key condition
	builder = builder.WithKeyCondition(
		itemTypeEquals(q.PartitionKeyValue).And(collectionSortKeyBetweenDates(
			q.CreatedAfter,
			q.CreatedBefore,
		)))
	// Apply the filter condition
	if q.Filter.IsSet() {
		builder = builder.WithFilter(q.Filter)
	}

	op = op.Modify(
		operation.QueryModifierFunc(func(ctx context.Context, qi *dynamodb.QueryInput) error {
			qi.IndexName = &opts.CollectionQueryIndexName
			return nil
		}),
		operation.WithExpressionBuilder(builder),
		operation.WithLimit(q.Limit),
	)
	if q.Cursor != "" {
		op = op.Modify(operation.WithLastToken(q.Cursor, q.StartKeyProvider))
	}
	return op
}

type LookupQuery struct {
	QueryOptions
}

func (q LookupQuery) modify(op operation.Query, opts Options) operation.Query {
	builder := expression.NewBuilder()
	keyCondition := hashKeyEquals(q.PartitionKeyValue)
	if q.SortKeyPrefix != "" {
		keyCondition = keyCondition.And(sortKeyStartsWith(q.SortKeyPrefix))
	}
	if q.Filter.IsSet() {
		builder = builder.WithFilter(q.Filter)
	}
	builder = builder.WithKeyCondition(keyCondition)
	op = op.Modify(
		operation.WithExpressionBuilder(builder),
		operation.WithLimit(q.Limit),
	)
	if q.Cursor != "" {
		op = op.Modify(operation.WithLastToken(q.Cursor, q.StartKeyProvider))
	}
	return op
}

func sortKeyStartsWith(prefix string) expression.KeyConditionBuilder {
	return expression.KeyBeginsWith(expression.Key(AttributeNameSK), prefix)
}

func hashKeyEquals(value string) expression.KeyConditionBuilder {
	return expression.KeyEqual(expression.Key(AttributeNameHK), expression.Value(value))
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

func updateTimestamp(ts time.Time) expression.UpdateBuilder {
	return expression.Set(expression.Name(AttributeNameUpdatedAt), expression.Value(ts))
}

func panicOnErr(err error, msg string, args ...any) {
	if err != nil {
		msg := fmt.Sprintf(msg, args...)
		panic(fmt.Errorf("%s: %w", msg, err))
	}
}
