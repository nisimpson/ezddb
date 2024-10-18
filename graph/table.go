package graph

import (
	"bytes"
	"context"
	"encoding/gob"
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

	AttributeNamePK                   = "pk"
	AttributeNameSK                   = "sk"
	AttributeNameItemType             = "itemType"
	AttributeNameCollectionSortKey    = "gsi2sk"
	AttributeNameReverseLookupSortKey = "gsi1sk"
	AttributeNameData                 = "data"
	AttributeNameCreatedAt            = "createdAt"
	AttributeNameUpdatedAt            = "updatedAt"
	AttributeNameExpires              = "expires"
)

var (
	FilterPK            = filter.AttributeOf(AttributeNamePK)
	FilterSK            = filter.AttributeOf(AttributeNameSK)
	FilterCreatedAt     = filter.AttributeOf(AttributeNameCreatedAt)
	FilterUpdatedAt     = filter.AttributeOf(AttributeNameUpdatedAt)
	FilterExpires       = filter.AttributeOf(AttributeNameExpires)
	FilterItemType      = filter.AttributeOf(AttributeNameItemType)
	FilterCollection    = filter.AttributeOf(AttributeNameCollectionSortKey)
	FilterReverseLookup = filter.AttributeOf(AttributeNameReverseLookupSortKey)
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
		EncodeDecode:             proxy.Default,
	}

	options.apply(opts)
	return newTable[T](options)
}

func newTable[T Data](options Options) Table[T] {
	return Table[T]{options: options}
}

type PaginationClient interface {
	ezddb.Getter
	ezddb.Putter
}

type Paginator struct {
	options Options
	client  PaginationClient
}

func NewPaginator(client PaginationClient, opts ...func(*Options)) Paginator {
	options := Options{
		TableName:                "pagination",
		ReverseLookupIndexName:   "reverse-lookup-index",
		CollectionQueryIndexName: "collection-query-index",
		Tick:                     time.Now,
	}

	options.apply(opts)
	return Paginator{options: options, client: client}
}

var (
	_ ezddb.StartKeyProvider      = Paginator{}
	_ ezddb.StartKeyTokenProvider = Paginator{}
)

type page struct {
	ID       string
	StartKey []byte
}

func (p page) DynamoItemType() string {
	return "Pagination"
}

func (p page) DynamoMarshalRecord(o *MarshalOptions) {
	o.HashID = p.ID
	o.SortID = p.ID
	o.HashPrefix = "pagination"
	o.SortPrefix = "pagination"
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

	out, err := table.Gets(page{ID: token}).Execute(ctx, p.client)
	if err != nil {
		return nil, fmt.Errorf("get page record from token '%s': %w", token, err)
	}

	record, err := table.Unmarshal(out.Item)
	if err != nil {
		return nil, fmt.Errorf("unmarshal page record from token '%s': %w", token, err)
	}

	var (
		data = record.Data.StartKey
		buf  = bytes.NewBuffer(data)
		item = ezddb.Item{}
	)

	err = p.options.EncodeDecode.Decode(gob.NewDecoder(buf), &item)
	if err != nil {
		return nil, fmt.Errorf("decode page token '%s': %w", token, err)
	}

	return item, nil
}

// GetStartKeyToken implements ezddb.StartKeyTokenProvider.
func (p Paginator) GetStartKeyToken(ctx context.Context, startKey map[string]types.AttributeValue) (string, error) {
	if len(startKey) == 0 {
		return "", nil
	}

	var (
		id    = p.options.IDGenerator.GenerateID(ctx)
		table = newTable[page](p.options)
		buf   = bytes.Buffer{}
	)

	err := p.options.EncodeDecode.Encode(gob.NewEncoder(&buf), startKey)
	if err != nil {
		return "", fmt.Errorf("get start key: encode: %w", err)
	}

	_, err = table.Puts(page{ID: id, StartKey: buf.Bytes()}).Execute(ctx, p.client)
	return id, err
}

func (t Table[T]) Puts(data T, opts ...func(*Options)) operation.PutOperation {
	t.options.apply(opts)
	record := Marshal(data, t.options.MarshalOptions...)
	item, err := t.options.Marshaler.MarshalMap(record)
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

type UpdateStrategy interface {
	modify(op operation.UpdateOperation, opts Options) operation.UpdateOperation
}

func (t Table[T]) Updates(id T, strategy UpdateStrategy, opts ...func(*Options)) operation.UpdateOperation {
	t.options.apply(opts)
	record := Marshal(id, t.options.MarshalOptions...)

	return strategy.modify(func(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
		return &dynamodb.UpdateItemInput{
			TableName:    &t.options.TableName,
			Key:          record.Key(),
			ReturnValues: types.ReturnValueAllNew,
		}, nil
	}, t.options)
}

type UpdateAttributeFunc = func(update expression.UpdateBuilder) expression.UpdateBuilder

type UpdateDataAttributes struct {
	Attributes []string
	Updates    map[string]UpdateAttributeFunc
}

func (u UpdateDataAttributes) modify(op operation.UpdateOperation, opts Options) operation.UpdateOperation {
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

func (t Table[T]) Unmarshal(item ezddb.Item, opts ...func(*Options)) (Record[T], error) {
	t.options.apply(opts)
	var record Record[T]
	err := t.options.Marshaler.UnmarshalMap(item, &record)
	return record, err
}

type ReverseLookupQuery struct {
	SortKeyValue      string
	GSI1SortKeyPrefix string
	Filter            expression.ConditionBuilder
	StartKeyProvider  ezddb.StartKeyProvider
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
	ItemType         string
	CreatedBefore    time.Time
	CreatedAfter     time.Time
	Filter           expression.ConditionBuilder
	StartKeyProvider ezddb.StartKeyProvider
	Cursor           string
	Limit            int
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
	PartitionKeyValue string
	SortKeyPrefix     string
	StartKeyProvider  ezddb.StartKeyProvider
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

func updateTimestamp(ts time.Time) expression.UpdateBuilder {
	return expression.Set(expression.Name(AttributeNameUpdatedAt), expression.Value(ts))
}
