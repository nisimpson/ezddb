package table

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/query"
	"github.com/nisimpson/ezddb/stored"
)

func init() {
	// register string attribute values for encoding and decoding of pagination
	// partition/sort keys.
	gob.Register(&types.AttributeValueMemberS{})
}

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
	AttributeHK            = query.Attribute(AttributeNameHK)
	AttributeSK            = query.Attribute(AttributeNameSK)
	AttributeCreatedAt     = query.Attribute(AttributeNameCreatedAt)
	AttributeUpdatedAt     = query.Attribute(AttributeNameUpdatedAt)
	AttributeExpires       = query.Attribute(AttributeNameExpires)
	AttributeItemType      = query.Attribute(AttributeNameItemType)
	AttributeCollection    = query.Attribute(AttributeNameCollectionSortKey)
	AttributeReverseLookup = query.Attribute(AttributeNameReverseLookupSortKey)
)

// Record represents a DynamoDB item in a [Table] with standard metadata
// fields and stronly typed data payloads.
type Record[T any] struct {
	HK        string    `dynamodbav:"hk"`               // HK is the hash key (partition key) for the DynamoDB item
	SK        string    `dynamodbav:"sk"`               // SK is the sort key (range key) for the DynamoDB item
	ItemType  string    `dynamodbav:"itemType"`         // ItemType identifies the type of item stored in the record
	GSI1SK    string    `dynamodbav:"gsi1Sk,omitempty"` // GSI1SK is the sort key for the reverse lookup Global Secondary Index
	GSI2SK    string    `dynamodbav:"gsi2Sk,omitempty"` // GSI2SK is the sort key for the collection query Global Secondary Index
	CreatedAt time.Time `dynamodbav:"createdAt"`        // CreatedAt stores the UTC timestamp when the record was created
	UpdatedAt time.Time `dynamodbav:"updatedAt"`        // UpdatedAt stores the UTC timestamp when the record was last modified
	Expires   time.Time `dynamodbav:"expires,unixtime"` // Expires stores the Unix timestamp when the record should expire
	Data      T         `dynamodbav:"data"`             // Data stores the record data. It is strongly typed for marshaling/unmarshaling.
}

// ItemKey returns the partition and sort key mapping that uniquely identifies
// this record in the [Table].
func (r Record[T]) ItemKey() ezddb.Item {
	return ezddb.Item{
		AttributeNameHK: &types.AttributeValueMemberS{Value: r.HK},
		AttributeNameSK: &types.AttributeValueMemberS{Value: r.SK},
	}
}

// clock tells the current time.
type clock func() time.Time

// MarshalOptions can configure the fields of [Record] generated by the
// [MarshalRecord] function.
type MarshalOptions struct {
	HashKeyID              string    // The id suffix of the partition key.
	SortKeyID              string    // The id suffix of the sort key.
	HashKeyPrefix          string    // The prefix of the partition key.
	SortKeyPrefix          string    // The prefix of the sort key.
	Delimiter              string    // The partition and sort key delimiter between the prefix and id.
	ItemType               string    // The record's item type.
	SupportReverseLookup   bool      // If true, then the value of [MarshalOptions.ReverseLookupSortKey] will be applied to the record.
	SupportCollectionQuery bool      // If true, then the value of [Record.GSI2SK] will be set to the current time.
	ReverseLookupSortKey   string    // The value assigned to [Record.GSI1SK] if [MarshalOptions.SupportReverseLookup] is true.
	Tick                   clock     // Returns the current time when invoked.
	ExpirationDate         time.Time // The date when this item is deleted from the [Table]. Zero value means no expiration.
}

// RecordMarshaler can generate [Record] instances that can be persisted to a [Table].
type RecordMarshaler interface {
	// DynamoMarshalRecord modifies [MarshalOptions] that determine the values of
	// fields within a [Record]. This method is invoked whenever [MarshalRecord]
	// is called on an instance of [RecordMarshaler].
	DynamoMarshalRecord(*MarshalOptions)
}

// MarshalRecord creates a new [Record] instance containing the target payload. Provide
// additional options to override the default configuration behavior.
func MarshalRecord[T RecordMarshaler](data T, opts ...func(*MarshalOptions)) Record[T] {
	options := MarshalOptions{
		Tick:      time.Now,
		Delimiter: DefaultDelimiter,
	}

	// marshal data
	data.DynamoMarshalRecord(&options)

	// Apply additional marshal options to override behavior
	for _, opt := range opts {
		opt(&options)
	}

	ts := options.Tick().UTC()

	record := Record[T]{
		HK:        options.HashKeyPrefix + options.Delimiter + options.HashKeyID,
		SK:        options.SortKeyPrefix + options.Delimiter + options.SortKeyID,
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
	MarshalOptions           []func(*MarshalOptions)
	Tick                     clock
	IDGenerator              IDGenerator
}

func (o *Options) Apply(opts []func(*Options)) {
	for _, opt := range opts {
		opt(o)
	}
}

// Table is a client for DynamoDB tables that adhere to following specifications:
//   - The parition and sort keys must be strings ([Record.HK] and [Record.SK], respectively).
//   - The first GSI should have [Record.SK] as its partition key, and [Record.GSI1SK] as the sort key.
//   - The second GSI should have [Record.ItemType] as its partition key, and [Record.GSI2SK] as the sort key.
//   - The Time-To-Live field should be [Record.Expires].
//
// Create new instances of Table with the [New] method:
//
//	type User struct {
//	    ID   string
//	    Name string
//	}
//
//	// Implement RecordMarshaler for User
//	func (u User) DynamoMarshalRecord(o *MarshalOptions) {
//	    o.HashKeyID = u.ID
//	    o.SortKeyID = u.ID
//	    o.HashKeyPrefix = "user"
//	    o.SortKeyPrefix = "user"
//	    o.ItemType = "users"
//	}
//
//	table := New[User]("users-table")
type Table[T RecordMarshaler] struct {
	Options Options
}

// New creates a new [Table] client.
func New[T RecordMarshaler](tableName string, opts ...func(*Options)) Table[T] {
	options := Options{
		TableName:                tableName,
		ReverseLookupIndexName:   "reverse-lookup-index",
		CollectionQueryIndexName: "collection-query-index",
		Tick:                     time.Now,
	}

	options.Apply(opts)
	return newTable[T](options)
}

func newTable[T RecordMarshaler](options Options) Table[T] {
	return Table[T]{Options: options}
}

// DynamoGetPutter implements the DynamoDB Get and Put APIs.
type DynamoGetPutter interface {
	ezddb.Getter
	ezddb.Putter
}

// Paginator stores pagination start keys in a [Table]. The stored
// keys are associated with a unique identifier; that identifier
// can be used to retrieve the start key and used in corresponding
// Query/Scan calls.
//
// Paginator instances are tightly coupled to their [Table], and
// be created via the [Table.Paginator] method.
type Paginator struct {
	options Options
	client  DynamoGetPutter
}

var (
	_ ezddb.StartKeyProvider      = Paginator{}
	_ ezddb.StartKeyTokenProvider = Paginator{}
)

type page struct {
	ID       string
	StartKey []byte
}

// DynamoMarshalRecord implements [RecordMarshaler].
func (p page) DynamoMarshalRecord(o *MarshalOptions) {
	o.HashKeyID = p.ID
	o.SortKeyID = p.ID
	o.HashKeyPrefix = "pagination"
	o.SortKeyPrefix = "pagination"
	o.ItemType = "pagination"
	o.SupportCollectionQuery = false
	o.SupportReverseLookup = false
}

// GetStartKey implements [ezddb.StartKeyProvider].
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
		buf  = bytes.NewBuffer(record.Data.StartKey)
		item = ezddb.Item{}
	)

	err = gob.NewDecoder(buf).Decode(&item)
	panicOnErr(err, "decode page token '%s'", token)

	return item, nil
}

// GetStartKeyToken implements [ezddb.StartKeyTokenProvider].
func (p Paginator) GetStartKeyToken(ctx context.Context, startKey map[string]types.AttributeValue) (string, error) {
	if len(startKey) == 0 {
		return "", nil
	}

	buf := bytes.Buffer{}
	err := gob.NewEncoder(&buf).Encode(startKey)
	panicOnErr(err, "get start key: encode")

	var (
		id    = p.options.IDGenerator.GenerateID(ctx)
		table = newTable[page](p.options)
	)

	_, err = table.Put(page{ID: id, StartKey: buf.Bytes()}).Execute(ctx, p.client)
	return id, err
}

// Paginator creates a new [Paginator] instance for this [Table].
func (t Table[T]) Paginator(client DynamoGetPutter) Paginator {
	return Paginator{client: client, options: t.Options}
}

// Put creates a new Put operation for storing a record in DynamoDB. It accepts the data
// to be stored and optional configuration options.
//
// Parameters:
//   - data: The data of type T that implements RecordMarshaler interface
//   - opts: Optional configuration functions to modify the table options
//
// Returns:
//   - stored.Put: A function that generates a DynamoDB PutItemInput when executed.
//
// Example:
//
//	user := User{ID: "123", Name: "John"}
//
//	// Create and execute the Put operation
//	putOp := table.Put(user)
//	result, err := putOp.Execute(ctx, client)
func (t Table[T]) Put(data T, opts ...func(*Options)) stored.Put {
	t.Options.Apply(opts)
	record := MarshalRecord(data, t.Options.MarshalOptions...)
	item, err := attributevalue.MarshalMap(record)
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		return &dynamodb.PutItemInput{
			TableName: &t.Options.TableName,
			Item:      item,
		}, err
	}
}

// Get creates a new Get operation for retrieving a record from DynamoDB. It accepts the data
// containing the key information and optional configuration options.
//
// Parameters:
//   - data: The data of type T that implements RecordMarshaler interface. Only the key fields
//     need to be populated as they will be used to construct the DynamoDB key
//   - opts: Optional configuration functions to modify the table options
//
// Returns:
//   - stored.Get: A function that generates a DynamoDB GetItemInput when executed
//
// Example:
//
//	type User struct {
//	    ID   string
//	    Name string
//	}
//
//	// Implement RecordMarshaler for User
//	func (u User) DynamoMarshalRecord(o *MarshalOptions) {
//	    o.HashKeyID = u.ID
//	    o.SortKeyID = u.ID
//	}
//
//	table := New[User]("users-table")
//
//	// Create and execute the Get operation
//	// Note: Only ID needs to be set for retrieval
//	getOp := table.Get(User{ID: "123"})
//	result, err := getOp.Execute(ctx, client)
//
//	if result.Item != nil {
//	    user, err := table.Unmarshal(result.Item)
//	    // user.Data contains the User record
//	}
func (t Table[T]) Get(data T, opts ...func(*Options)) stored.Get {
	t.Options.Apply(opts)
	record := MarshalRecord(data, t.Options.MarshalOptions...)
	key := record.ItemKey()
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		return &dynamodb.GetItemInput{
			TableName: &t.Options.TableName,
			Key:       key,
		}, nil
	}
}

// Delete creates a new Delete operation for removing a record from DynamoDB. It accepts the data
// containing the key information and optional configuration options.
//
// Parameters:
//   - data: The data of type T that implements RecordMarshaler interface. Only the key fields
//     need to be populated as they will be used to construct the DynamoDB key
//   - opts: Optional configuration functions to modify the table options
//
// Returns:
//   - stored.Delete: A function that generates a DynamoDB DeleteItemInput when executed
//
// Example:
//
//	type User struct {
//	    ID   string
//	    Name string
//	}
//
//	// Implement RecordMarshaler for User
//	func (u User) DynamoMarshalRecord(o *MarshalOptions) {
//	    o.HashKeyID = u.ID
//	    o.SortKeyID = u.ID
//	}
//
//	table := New[User]("users-table")
//
//	// Create and execute the Delete operation
//	// Note: Only ID needs to be set for deletion
//	deleteOp := table.Delete(User{ID: "123"})
//	result, err := deleteOp.Execute(ctx, client)
func (t Table[T]) Delete(data T, opts ...func(*Options)) stored.Delete {
	t.Options.Apply(opts)
	record := MarshalRecord(data, t.Options.MarshalOptions...)
	key := record.ItemKey()
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		return &dynamodb.DeleteItemInput{
			TableName: &t.Options.TableName,
			Key:       key,
		}, nil
	}
}

// UpdateStrategy defines a mechanism for updating a [Record] in a [Table].
type UpdateStrategy interface {
	modify(op stored.UpdateItem, opts Options) stored.UpdateItem
}

// Update creates a new Update operation for modifying an existing record in DynamoDB. It accepts
// the key information and an UpdateStrategy that defines how the record should be modified.
//
// Parameters:
//   - id: The data of type T that implements RecordMarshaler interface. Only the key fields
//     need to be populated as they will be used to construct the DynamoDB key
//   - strategy: An UpdateStrategy implementation that defines how to modify the record
//   - opts: Optional configuration functions to modify the table options
//
// Returns:
//   - stored.UpdateItem: A function that generates a DynamoDB UpdateItemInput when executed
//
// Example:
//
//	type User struct {
//	    ID   string
//	    Name string
//	    Age  int
//	}
//
//	// Create update strategy for specific attributes
//	updateStrategy := UpdateDataAttributes{
//	    Attributes: []string{"Name", "Age"},
//	    Updates: map[string]UpdateAttributeFunc{
//	        "Name": func(update expression.UpdateBuilder) expression.UpdateBuilder {
//	            return update.Set(expression.Name("data.name"), expression.Value("Jane"))
//	        },
//	        "Age": func(update expression.UpdateBuilder) expression.UpdateBuilder {
//	            return update.Set(expression.Name("data.age"), expression.Value(25))
//	        },
//	    },
//	}
//
//	// Create and execute the Update operation
//	updateOp := table.Update(User{ID: "123"}, updateStrategy)
//	result, err := updateOp.Execute(ctx, client)
func (t Table[T]) Update(id T, strategy UpdateStrategy, opts ...func(*Options)) stored.UpdateItem {
	t.Options.Apply(opts)
	record := MarshalRecord(id, t.Options.MarshalOptions...)

	return strategy.modify(func(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
		return &dynamodb.UpdateItemInput{
			TableName:    &t.Options.TableName,
			Key:          record.ItemKey(),
			ReturnValues: types.ReturnValueAllNew,
		}, nil
	}, t.Options)
}

// UpdateAttribute is a function that modifies an [expression.UpdateBuilder].
type UpdateAttribute = func(update expression.UpdateBuilder) expression.UpdateBuilder

// UpdateAttributes provides a structure for updating specific fields, or attributes
// on a data [Record].
type UpdateAttributes struct {
	Attributes []string // The list of attributes to update on the [Record].

	// A mapping of attributes to their associated attribute updater function.
	Updates map[string]UpdateAttribute
}

func (u UpdateAttributes) modify(op stored.UpdateItem, opts Options) stored.UpdateItem {
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
	return op.Modify(stored.WithExpressionBuilder(builder))
}

// QueryStrategy modifies Query requests on a [Table].
type QueryStrategy interface {
	modify(op stored.Query, opts Options) stored.Query
}

// Query creates a new Query operation for retrieving multiple records from DynamoDB based on
// the provided QueryStrategy. The strategy determines how the query will be constructed and
// which index (if any) will be used.
//
// Parameters:
//   - strategy: A QueryStrategy implementation that defines how to construct the query
//   - opts: Optional configuration functions to modify the table options
//
// Returns:
//   - stored.Query: A function that generates a DynamoDB QueryInput when executed
//
// Example:
//
//	// Query using reverse lookup (GSI1)
//	reverseLookup := ReverseLookupQuery{
//	    QueryOptions: QueryOptions{
//	        PartitionKeyValue: "user#123",
//	        SortKeyPrefix: "order#",
//	        Limit: 10,
//	        Filter: expression.Name("status").Equal(expression.Value("active")),
//	    },
//	}
//
//	queryOp := table.Query(reverseLookup)
//	result, err := queryOp.Execute(ctx, client)
//
//	// Query using collection index (GSI2)
//	collectionQuery := CollectionQuery{
//	    QueryOptions: QueryOptions{
//	        PartitionKeyValue: "order",
//	        CreatedAfter: time.Now().Add(-24 * time.Hour),
//	        Limit: 20,
//	    },
//	}
//
//	queryOp := table.Query(collectionQuery)
//	result, err := queryOp.Execute(ctx, client)
func (t Table[T]) Query(strategy QueryStrategy, opts ...func(*Options)) stored.Query {
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
	err := attributevalue.UnmarshalMap(item, &record)
	return record, err
}

func (t Table[T]) DataAttribute(name string) query.ItemAttribute {
	return query.Attribute("data", name)
}

// QueryOptions configure constraints on [Table] queries.
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

	CreatedBefore    time.Time                   // Upper bound for creation date (collection queries)
	CreatedAfter     time.Time                   // Lower bound for creation date (collection queries)
	Filter           expression.ConditionBuilder // A filter condition to Apply to the query.
	StartKeyProvider ezddb.StartKeyProvider      // The start key provider used for pagination.
	Cursor           string                      // The last pagination cursor token.
	Limit            int                         // Maximum number of items to return.
}

// ReverseLookupQuery performs queries using the reverse lookup index (GSI1).
// It can be supplied as a [QueryStrategy] in the [Table.Query] method.
type ReverseLookupQuery struct {
	QueryOptions
}

func (q ReverseLookupQuery) modify(op stored.Query, opts Options) stored.Query {
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
		stored.QueryModifierFunc(func(ctx context.Context, qi *dynamodb.QueryInput) error {
			qi.IndexName = &opts.ReverseLookupIndexName
			return nil
		}),
		stored.WithExpressionBuilder(builder),
		stored.WithLimit(q.Limit),
	)
	if q.Cursor != "" {
		op = op.Modify(stored.WithLastToken(q.Cursor, q.StartKeyProvider))
	}
	return op
}

// CollectionQuery performs queries using the collection index (GSI2).
// It can be supplied as a [QueryStrategy] in the [Table.Query] method.
type CollectionQuery struct {
	QueryOptions
}

func (q CollectionQuery) modify(op stored.Query, opts Options) stored.Query {
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
		stored.QueryModifierFunc(func(ctx context.Context, qi *dynamodb.QueryInput) error {
			qi.IndexName = &opts.CollectionQueryIndexName
			return nil
		}),
		stored.WithExpressionBuilder(builder),
		stored.WithLimit(q.Limit),
	)
	if q.Cursor != "" {
		op = op.Modify(stored.WithLastToken(q.Cursor, q.StartKeyProvider))
	}
	return op
}

// LookupQuery performs queries on the [Table].
// It can be supplied as a [QueryStrategy] for the [Table.Query] method.
type LookupQuery struct {
	QueryOptions
}

func (q LookupQuery) modify(op stored.Query, opts Options) stored.Query {
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
		stored.WithExpressionBuilder(builder),
		stored.WithLimit(q.Limit),
	)
	if q.Cursor != "" {
		op = op.Modify(stored.WithLastToken(q.Cursor, q.StartKeyProvider))
	}
	return op
}

func sortKeyStartsWith(prefix string) expression.KeyConditionBuilder {
	return expression.KeyBeginsWith(expression.Key(AttributeNameSK), prefix)
}

func hashKeyEquals(value string) expression.KeyConditionBuilder {
	if value == "" {
		panic(errors.New("hash key cannot be empty"))
	}
	return expression.KeyEqual(expression.Key(AttributeNameHK), expression.Value(value))
}

func sortKeyEquals(value string) expression.KeyConditionBuilder {
	if value == "" {
		panic(errors.New("sort key cannot be empty"))
	}
	return expression.KeyEqual(expression.Key(AttributeNameSK), expression.Value(value))
}

func reverseLookupSortKeyStartsWith(prefix string) expression.KeyConditionBuilder {
	return expression.KeyBeginsWith(expression.Key(AttributeNameReverseLookupSortKey), prefix)
}

func itemTypeEquals(value string) expression.KeyConditionBuilder {
	if value == "" {
		panic(errors.New("item type cannot be empty"))
	}
	return expression.KeyEqual(expression.Key(AttributeNameItemType), expression.Value(value))
}

func collectionSortKeyBetweenDates(start, end time.Time) expression.KeyConditionBuilder {
	if start.After(end) {
		panic(errors.New("start date must be before end date"))
	}
	if end.IsZero() {
		panic(errors.New("end date must be non-zero"))
	}
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
