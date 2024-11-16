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

	AttributeNameHK                   = "hk"
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
	FilterPK            = filter.AttributeOf(AttributeNameHK)
	FilterSK            = filter.AttributeOf(AttributeNameSK)
	FilterCreatedAt     = filter.AttributeOf(AttributeNameCreatedAt)
	FilterUpdatedAt     = filter.AttributeOf(AttributeNameUpdatedAt)
	FilterExpires       = filter.AttributeOf(AttributeNameExpires)
	FilterItemType      = filter.AttributeOf(AttributeNameItemType)
	FilterCollection    = filter.AttributeOf(AttributeNameCollectionSortKey)
	FilterReverseLookup = filter.AttributeOf(AttributeNameReverseLookupSortKey)
)

// Entity2 represents a singular "thing" within an entity-relationship model.
// Entities can form relationships with other entities; these associations
// are stored within the dynamodb table as separate rows within the table.
// Use the [Graph] client to perform CRUD operations on stored entities.
type Entity interface {
	DynamoID() string
	DynamoItemType() string
	DynamoMarshalRecord(*MarshalOptions)
}

type Record[T Entity] struct {
	HK        string    `dynamodbav:"hk"`
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
	SupportReverseLookup   bool
	SupportCollectionQuery bool
	ReverseLookupSortKey   string
	Tick                   clock
	ExpirationDate         time.Time
}

func Marshal[T Entity](data T, opts ...func(*MarshalOptions)) Record[T] {
	options := MarshalOptions{
		HashKeyID:     data.DynamoID(),
		HashKeyPrefix: data.DynamoItemType(),
		SortKeyID:     data.DynamoID(),
		SortKeyPrefix: data.DynamoItemType(),
		Tick:          time.Now,
	}

	// marshal data
	data.DynamoMarshalRecord(&options)

	// apply additional marshal options to override behavior
	for _, opt := range opts {
		opt(&options)
	}

	ts := options.Tick().UTC()

	record := Record[T]{
		HK:        options.HashKeyPrefix + DefaultDelimiter + options.HashKeyID,
		SK:        options.SortKeyPrefix + DefaultDelimiter + options.SortKeyID,
		ItemType:  data.DynamoItemType(),
		Data:      &data,
		Expires:   options.ExpirationDate,
		CreatedAt: ts,
		UpdatedAt: ts,
	}

	if options.SupportReverseLookup {
		record.GSI1SK = options.ReverseLookupSortKey
		record.GSI1SK = record.HK

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

func (o *Options) apply(opts []func(*Options)) {
	for _, opt := range opts {
		opt(o)
	}
}

type Table[T Entity] struct {
	options Options
}

func NewTable[T Entity](tableName string, opts ...func(*Options)) Table[T] {
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

func newTable[T Entity](options Options) Table[T] {
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

var (
	_ ezddb.StartKeyProvider      = Paginator{}
	_ ezddb.StartKeyTokenProvider = Paginator{}
)

type page struct {
	ID       string
	StartKey []byte
}

func (p page) DynamoID() string { return p.ID }

func (p page) DynamoItemType() string { return "Pagination" }

func (p page) DynamoMarshalRecord(o *MarshalOptions) {
	o.HashKeyID = p.ID
	o.SortKeyID = p.ID
	o.HashKeyPrefix = "pagination"
	o.SortKeyPrefix = "pagination"
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

	_, err = table.Put(page{ID: id, StartKey: buf.Bytes()}).Execute(ctx, p.client)
	return id, err
}

func (t Table[T]) Paginator(client PaginationClient) Paginator {
	return Paginator{client: client, options: t.options}
}

func (t Table[T]) Put(data T, opts ...func(*Options)) operation.Put {
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

func (t Table[T]) Get(data T, opts ...func(*Options)) operation.Get {
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

func (t Table[T]) Delete(data T, opts ...func(*Options)) operation.Delete {
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
	modify(op operation.UpdateItem, opts Options) operation.UpdateItem
}

func (t Table[T]) Update(id T, strategy UpdateStrategy, opts ...func(*Options)) operation.UpdateItem {
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

func (q ReverseLookupQuery) modify(op operation.Query, opts Options) operation.Query {
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

func (q CollectionQuery) modify(op operation.Query, opts Options) operation.Query {
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

func (q LookupQuery) modify(op operation.Query, opts Options) operation.Query {
	builder := expression.NewBuilder()
	keyCondition := hashKeyEquals(q.PartitionKeyValue)
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
