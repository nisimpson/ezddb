package table

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockDynamoClient implements DynamoGetPutter for testing
type MockDynamoClient struct {
	mock.Mock
}

func (m *MockDynamoClient) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.GetItemOutput), args.Error(1)
}

func (m *MockDynamoClient) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.PutItemOutput), args.Error(1)
}

func (m *MockDynamoClient) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.DeleteItemOutput), args.Error(1)
}

func (m *MockDynamoClient) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.QueryOutput), args.Error(1)
}

// TestData implements RecordMarshaler for testing
type TestData struct {
	ID   string
	Name string
}

func (d TestData) DynamoMarshalRecord(o *MarshalOptions) {
	o.ItemType = "test"
	o.HashKeyPrefix = "test#"
	o.SortKeyPrefix = "data#"
}

func TestRecord(t *testing.T) {
	t.Run("ItemKey returns correct key structure", func(t *testing.T) {
		data := TestData{ID: "test-id", Name: "test"}
		record := Record[TestData]{
			HK:        "test#test-id",
			SK:        "data#",
			Data:      data,
			CreatedAt: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			ItemType:  "test",
		}

		key := record.ItemKey()
		assert.Equal(t, &types.AttributeValueMemberS{Value: "test#test-id"}, key["hk"])
		assert.Equal(t, &types.AttributeValueMemberS{Value: "data#"}, key["sk"])
	})
}

func TestMarshalRecord(t *testing.T) {
	t.Run("marshals record with collection query support", func(t *testing.T) {
		data := TestData{ID: "test-id", Name: "test-name"}
		now := time.Now()

		record := MarshalRecord(data, func(o *MarshalOptions) {
			o.Tick = func() time.Time { return now }
			o.SupportCollectionQuery = true
			o.ItemType = "test"
			o.HashKeyID = data.ID
			o.HashKeyPrefix = "test"
			o.Delimiter = "#"
			o.SortKeyPrefix = "data"
		})

		assert.Equal(t, "test#test-id", record.HK)
		assert.Equal(t, "data#", record.SK)
		assert.Equal(t, "test", record.ItemType)
		assert.NotEmpty(t, record.GSI2SK)
		createdAtTime, err := time.Parse(time.RFC3339, record.GSI2SK)
		require.NoError(t, err)
		assert.Equal(t, now.UTC(), createdAtTime.UTC())
	})
	t.Run("marshals record with default options", func(t *testing.T) {
		data := TestData{ID: "test-id", Name: "test-name"}
		now := time.Now()

		record := MarshalRecord(data, func(o *MarshalOptions) {
			o.Tick = func() time.Time { return now }
		})

		assert.Equal(t, "test#test-id", record.HK)
		assert.Equal(t, "data#", record.SK)
		assert.Equal(t, "test", record.ItemType)
		assert.Equal(t, now, record.CreatedAt)
		assert.Equal(t, now, record.UpdatedAt)
		assert.Equal(t, data, record.Data)
	})

	t.Run("marshals record with custom options", func(t *testing.T) {
		data := TestData{ID: "test-id", Name: "test-name"}
		now := time.Now()
		expires := now.Add(24 * time.Hour)

		record := MarshalRecord(data, func(o *MarshalOptions) {
			o.Tick = func() time.Time { return now }
			o.HashKeyPrefix = "custom#"
			o.SortKeyPrefix = "sort#"
			o.ExpirationDate = expires
			o.SupportReverseLookup = true
			o.ReverseLookupSortKey = "reverse#key"
		})

		assert.Equal(t, "custom#test-id", record.HK)
		assert.Equal(t, "sort#", record.SK)
		assert.Equal(t, "reverse#key", record.GSI1SK)
		assert.Equal(t, expires, record.Expires)
	})
}

func TestMarshalOptions(t *testing.T) {
	t.Run("defaults are set correctly", func(t *testing.T) {
		opts := MarshalOptions{}
		assert.Empty(t, opts.HashKeyID)
		assert.Empty(t, opts.SortKeyID)
		assert.Empty(t, opts.HashKeyPrefix)
		assert.Empty(t, opts.SortKeyPrefix)
		assert.Empty(t, opts.ItemType)
		assert.False(t, opts.SupportReverseLookup)
		assert.False(t, opts.SupportCollectionQuery)
		assert.Empty(t, opts.ReverseLookupSortKey)
		assert.Zero(t, opts.ExpirationDate)
	})

	t.Run("applies values correctly", func(t *testing.T) {
		now := time.Now()
		opts := MarshalOptions{
			HashKeyID:            "hash-id",
			SortKeyID:            "sort-id",
			HashKeyPrefix:        "hash#",
			SortKeyPrefix:        "sort#",
			Delimiter:            "-",
			ItemType:             "custom",
			SupportReverseLookup: true,
			ReverseLookupSortKey: "reverse#",
			Tick:                 func() time.Time { return now },
			ExpirationDate:       now.Add(time.Hour),
		}

		assert.Equal(t, "hash-id", opts.HashKeyID)
		assert.Equal(t, "sort-id", opts.SortKeyID)
		assert.Equal(t, "hash#", opts.HashKeyPrefix)
		assert.Equal(t, "sort#", opts.SortKeyPrefix)
		assert.Equal(t, "-", opts.Delimiter)
		assert.Equal(t, "custom", opts.ItemType)
		assert.True(t, opts.SupportReverseLookup)
		assert.Equal(t, "reverse#", opts.ReverseLookupSortKey)
		assert.Equal(t, now, opts.Tick())
		assert.Equal(t, now.Add(time.Hour), opts.ExpirationDate)
	})
}

func TestUnmarshal(t *testing.T) {
	t.Run("unmarshals item correctly", func(t *testing.T) {
		item := map[string]types.AttributeValue{
			"data": &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"ID":   &types.AttributeValueMemberS{Value: "test-id"},
					"Name": &types.AttributeValueMemberS{Value: "test-name"},
				},
			},
		}

		table := New[TestData]("test-table")
		record, err := table.Unmarshal(item)
		require.NoError(t, err)
		assert.Equal(t, "test-id", record.Data.ID)
		assert.Equal(t, "test-name", record.Data.Name)
	})

	t.Run("returns error for invalid item", func(t *testing.T) {
		item := map[string]types.AttributeValue{
			"data": &types.AttributeValueMemberS{Value: "invalid"},
		}

		table := New[TestData]("test-table")
		_, err := table.Unmarshal(item)
		require.Error(t, err)
	})
}

func TestTable(t *testing.T) {
	t.Run("New creates table with default options", func(t *testing.T) {
		table := New[TestData]("test-table")
		assert.Equal(t, "test-table", table.Options.TableName)
		assert.Equal(t, "reverse-lookup-index", table.Options.ReverseLookupIndexName)
		assert.Equal(t, "collection-query-index", table.Options.CollectionQueryIndexName)
		assert.NotNil(t, table.Options.Tick)
	})

	t.Run("New applies custom options", func(t *testing.T) {
		customClock := func() time.Time { return time.Time{} }
		customTableName := "custom-table"

		table := New[TestData](customTableName, func(o *Options) {
			o.Tick = customClock
			o.ReverseLookupIndexName = "custom-reverse"
			o.CollectionQueryIndexName = "custom-collection"
		})

		assert.Equal(t, customTableName, table.Options.TableName)
		assert.Equal(t, "custom-reverse", table.Options.ReverseLookupIndexName)
		assert.Equal(t, "custom-collection", table.Options.CollectionQueryIndexName)
		assert.NotNil(t, table.Options.Tick)
	})
}

func TestConcurrency(t *testing.T) {
	t.Run("concurrent table operations are safe", func(t *testing.T) {
		table := New[TestData]("test-table")
		data := TestData{ID: "test-id", Name: "test-name"}

		// Run multiple operations concurrently
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func() {
				table.Put(data)
				table.Get(data)
				table.Query(LookupQuery{
					QueryOptions: QueryOptions{
						PartitionKeyValue: "test#123",
						SortKeyPrefix:     "data#456",
					},
				})
				done <- true
			}()
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestPaginatorEncoding(t *testing.T) {
	t.Run("encodes and decodes paginator token with real data", func(t *testing.T) {
		// Create a realistic start key
		startKey := map[string]types.AttributeValue{
			"hk":     &types.AttributeValueMemberS{Value: "test#123"},
			"sk":     &types.AttributeValueMemberS{Value: "data#456"},
			"gsi1sk": &types.AttributeValueMemberS{Value: "index#789"},
		}

		// Create paginator
		p := Paginator{
			options: Options{TableName: "test-table"},
		}

		// Get token
		ctx := context.Background()
		token, err := p.GetStartKeyToken(ctx, startKey)
		require.NoError(t, err)
		require.NotEmpty(t, token)

		// Decode token back to start key
		decodedKey, err := p.GetStartKey(ctx, token)
		require.NoError(t, err)
		require.NotNil(t, decodedKey)

		// Verify key values are preserved
		hkAttr, ok := decodedKey["hk"].(*types.AttributeValueMemberS)
		require.True(t, ok)
		assert.Equal(t, "test#123", hkAttr.Value)

		skAttr, ok := decodedKey["sk"].(*types.AttributeValueMemberS)
		require.True(t, ok)
		assert.Equal(t, "data#456", skAttr.Value)
	})
	t.Run("encodes and decodes paginator token", func(t *testing.T) {
		// Create page data
		pageData := page{
			ID:       "test-id",
			StartKey: []byte("test-key"),
		}

		// Convert to JSON
		jsonData, err := json.Marshal(pageData)
		require.NoError(t, err)

		// Encode to base64
		token := base64.URLEncoding.EncodeToString(jsonData)
		require.NotEmpty(t, token)

		// Decode token
		decoded, err := base64.URLEncoding.DecodeString(token)
		require.NoError(t, err)

		// Parse JSON
		var decodedPage page
		err = json.Unmarshal(decoded, &decodedPage)
		require.NoError(t, err)

		assert.Equal(t, pageData.ID, decodedPage.ID)
		assert.Equal(t, pageData.StartKey, decodedPage.StartKey)
	})

	t.Run("handles corrupted token", func(t *testing.T) {
		p := Paginator{
			options: Options{TableName: "test-table"},
		}

		ctx := context.Background()
		_, err := p.GetStartKey(ctx, "invalid base64!")
		require.Error(t, err)
	})
}

func TestPaginator(t *testing.T) {
	t.Run("GetStartKey with valid token", func(t *testing.T) {
		p := Paginator{
			options: Options{TableName: "test-table"},
		}

		ctx := context.Background()
		key, err := p.GetStartKey(ctx, "validtoken")
		require.NoError(t, err)
		assert.NotNil(t, key)
	})

	t.Run("GetStartKey with empty token", func(t *testing.T) {
		p := Paginator{
			options: Options{TableName: "test-table"},
		}

		ctx := context.Background()
		key, err := p.GetStartKey(ctx, "")
		require.NoError(t, err)
		assert.Nil(t, key)
	})

	t.Run("GetStartKey with invalid token", func(t *testing.T) {
		p := Paginator{
			options: Options{TableName: "test-table"},
		}

		ctx := context.Background()
		_, err := p.GetStartKey(ctx, "invalid-token")
		require.Error(t, err)
	})

	t.Run("GetStartKeyToken with valid start key", func(t *testing.T) {
		p := Paginator{
			options: Options{TableName: "test-table"},
		}

		ctx := context.Background()
		startKey := map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: "test-id"},
		}

		token, err := p.GetStartKeyToken(ctx, startKey)
		require.NoError(t, err)
		assert.NotEmpty(t, token)
	})

	t.Run("GetStartKeyToken with nil start key", func(t *testing.T) {
		p := Paginator{
			options: Options{TableName: "test-table"},
		}

		ctx := context.Background()
		token, err := p.GetStartKeyToken(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, token)
	})
}

func TestUpdateStrategies(t *testing.T) {
	t.Run("UpdateDataAttributes sets values and timestamps", func(t *testing.T) {
		table := New[TestData]("test-table")
		now := time.Now()
		table.Options.Tick = func() time.Time { return now }

		update := UpdateDataAttributes{
			Attributes: []string{"name", "age"},
			Updates: map[string]UpdateAttributeFunc{
				"name": func(update expression.UpdateBuilder) expression.UpdateBuilder {
					return update.Set(table.DataAttribute("name").Name(), expression.Value("new-name"))
				},
				"age": func(update expression.UpdateBuilder) expression.UpdateBuilder {
					return update.Set(table.DataAttribute("age").Name(), expression.Value(30))
				},
			},
		}

		updateOp := table.Update(TestData{ID: "test-id"}, update)
		require.NotNil(t, updateOp)
	})

	t.Run("Update with invalid strategy panics", func(t *testing.T) {
		table := New[TestData]("test-table")
		data := TestData{ID: "test-id"}

		assert.Panics(t, func() {
			table.Update(data, nil)
		})
	})
}

func TestErrorHandling(t *testing.T) {
	t.Run("panics on nil options", func(t *testing.T) {
		assert.Panics(t, func() {
			panicOnErr(fmt.Errorf("test error"), "test message")
		})
	})

	t.Run("handles invalid query parameters", func(t *testing.T) {
		query := ReverseLookupQuery{
			QueryOptions: QueryOptions{
				PartitionKeyValue: "", // Invalid empty value
				SortKeyPrefix:     "",
			},
		}

		table := New[TestData]("test-table")
		queryOp := table.Query(query)
		assert.NotNil(t, queryOp)
	})
}

func TestOptionsValidation(t *testing.T) {
	t.Run("applies option functions", func(t *testing.T) {
		opts := Options{TableName: "test-table"}

		funcs := []func(*Options){
			func(o *Options) { o.ReverseLookupIndexName = "custom-index" },
		}

		opts.Apply(funcs)
		assert.Equal(t, "custom-index", opts.ReverseLookupIndexName)
	})
}

func TestExpressionBuilders(t *testing.T) {
	t.Run("key conditions with empty values", func(t *testing.T) {
		assert.Panics(t, func() {
			hashKeyEquals("")
		})

		assert.Panics(t, func() {
			sortKeyEquals("")
		})

		assert.Panics(t, func() {
			sortKeyStartsWith("")
		})
	})

	t.Run("date range conditions with invalid times", func(t *testing.T) {
		start := time.Now()
		end := start.Add(-time.Hour) // End before start

		assert.Panics(t, func() {
			collectionSortKeyBetweenDates(start, end)
		})

		assert.Panics(t, func() {
			collectionSortKeyBetweenDates(time.Time{}, time.Time{})
		})
	})
	t.Run("hashKeyEquals builds correct condition", func(t *testing.T) {
		cond := hashKeyEquals("test-id")
		assert.NotNil(t, cond)
	})

	t.Run("sortKeyEquals builds correct condition", func(t *testing.T) {
		cond := sortKeyEquals("test-sort")
		assert.NotNil(t, cond)
	})

	t.Run("sortKeyStartsWith builds correct condition", func(t *testing.T) {
		cond := sortKeyStartsWith("prefix#")
		assert.NotNil(t, cond)
	})

	t.Run("reverseLookupSortKeyStartsWith builds correct condition", func(t *testing.T) {
		cond := reverseLookupSortKeyStartsWith("prefix#")
		assert.NotNil(t, cond)
	})

	t.Run("itemTypeEquals builds correct condition", func(t *testing.T) {
		cond := itemTypeEquals("test-type")
		assert.NotNil(t, cond)
	})

	t.Run("collectionSortKeyBetweenDates builds correct condition", func(t *testing.T) {
		start := time.Now().Add(-time.Hour)
		end := time.Now()
		cond := collectionSortKeyBetweenDates(start, end)
		assert.NotNil(t, cond)
	})
}

func TestOperationsExecution(t *testing.T) {
	t.Run("Delete executes successfully", func(t *testing.T) {
		mockClient := &MockDynamoClient{}
		mockClient.On("DeleteItem", mock.Anything, mock.Anything, mock.Anything).
			Return(&dynamodb.DeleteItemOutput{}, nil)

		data := TestData{ID: "test-id", Name: "test-name"}
		table := New[TestData]("test-table")
		deleteOp := table.Delete(data)

		_, err := deleteOp.Execute(context.Background(), mockClient)
		require.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("Delete fails with error", func(t *testing.T) {
		mockClient := &MockDynamoClient{}
		mockClient.On("DeleteItem", mock.Anything, mock.Anything, mock.Anything).
			Return(&dynamodb.DeleteItemOutput{}, errors.New("delete failed"))

		data := TestData{ID: "test-id", Name: "test-name"}
		table := New[TestData]("test-table")
		deleteOp := table.Delete(data)

		_, err := deleteOp.Execute(context.Background(), mockClient)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "delete failed")
		mockClient.AssertExpectations(t)
	})

	t.Run("Query executes successfully", func(t *testing.T) {
		mockClient := &MockDynamoClient{}
		mockClient.On("Query", mock.Anything, mock.Anything, mock.Anything).
			Return(&dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						"data": &types.AttributeValueMemberM{
							Value: map[string]types.AttributeValue{
								"ID":   &types.AttributeValueMemberS{Value: "test-id"},
								"Name": &types.AttributeValueMemberS{Value: "test-name"},
							},
						},
					},
				},
				LastEvaluatedKey: map[string]types.AttributeValue{
					"hk": &types.AttributeValueMemberS{Value: "test#last-id"},
				},
			}, nil)

		table := New[TestData]("test-table")
		queryOp := table.Query(ReverseLookupQuery{
			QueryOptions: QueryOptions{
				PartitionKeyValue: "test-id",
				SortKeyPrefix:     "data#",
			},
		})

		result, err := queryOp.Execute(context.Background(), mockClient)
		require.NoError(t, err)
		assert.NotEmpty(t, result.Items)
		assert.NotNil(t, result.LastEvaluatedKey)
		mockClient.AssertExpectations(t)
	})

	t.Run("Query fails with error", func(t *testing.T) {
		mockClient := &MockDynamoClient{}
		mockClient.On("Query", mock.Anything, mock.Anything, mock.Anything).
			Return(&dynamodb.QueryOutput{}, errors.New("query failed"))

		table := New[TestData]("test-table")
		queryOp := table.Query(ReverseLookupQuery{
			QueryOptions: QueryOptions{
				PartitionKeyValue: "test-id",
				SortKeyPrefix:     "data#",
			},
		})

		_, err := queryOp.Execute(context.Background(), mockClient)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "query failed")
		mockClient.AssertExpectations(t)
	})
	t.Run("Put executes successfully", func(t *testing.T) {
		mockClient := &MockDynamoClient{}
		mockClient.On("PutItem", mock.Anything, mock.Anything, mock.Anything).
			Return(&dynamodb.PutItemOutput{}, nil)

		data := TestData{ID: "test-id", Name: "test-name"}
		table := New[TestData]("test-table")
		putOp := table.Put(data)

		_, err := putOp.Execute(context.Background(), mockClient)
		require.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("Put fails with error", func(t *testing.T) {
		mockClient := &MockDynamoClient{}
		mockClient.On("PutItem", mock.Anything, mock.Anything, mock.Anything).
			Return(&dynamodb.PutItemOutput{}, errors.New("put failed"))

		data := TestData{ID: "test-id", Name: "test-name"}
		table := New[TestData]("test-table")
		putOp := table.Put(data)

		_, err := putOp.Execute(context.Background(), mockClient)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "put failed")
		mockClient.AssertExpectations(t)
	})

	t.Run("Get executes successfully", func(t *testing.T) {
		mockClient := &MockDynamoClient{}
		mockClient.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
			Return(&dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"data": &types.AttributeValueMemberM{
						Value: map[string]types.AttributeValue{
							"ID":   &types.AttributeValueMemberS{Value: "test-id"},
							"Name": &types.AttributeValueMemberS{Value: "test-name"},
						},
					},
				},
			}, nil)

		data := TestData{ID: "test-id", Name: "test-name"}
		table := New[TestData]("test-table")
		getOp := table.Get(data)

		result, err := getOp.Execute(context.Background(), mockClient)
		require.NoError(t, err)
		assert.NotNil(t, result)
		mockClient.AssertExpectations(t)
	})
}

func TestOperations(t *testing.T) {
	t.Run("Put creates correct operation", func(t *testing.T) {
		data := TestData{ID: "test-id", Name: "test-name"}
		table := New[TestData]("test-table")

		putOp := table.Put(data)
		assert.NotNil(t, putOp)

		// Test with custom options
		customOp := table.Put(data, func(o *Options) {
			o.TableName = "custom-table"
		})
		assert.NotNil(t, customOp)
	})

	t.Run("Get creates correct operation", func(t *testing.T) {
		data := TestData{ID: "test-id", Name: "test-name"}
		table := New[TestData]("test-table")

		getOp := table.Get(data)
		assert.NotNil(t, getOp)

		// Test with custom options
		customOp := table.Get(data, func(o *Options) {})
		assert.NotNil(t, customOp)
	})

	t.Run("Delete creates correct operation", func(t *testing.T) {
		data := TestData{ID: "test-id", Name: "test-name"}
		table := New[TestData]("test-table")

		deleteOp := table.Delete(data)
		assert.NotNil(t, deleteOp)

		// Test with custom options
		customOp := table.Delete(data, func(o *Options) {
			o.TableName = "custom-table"
		})
		assert.NotNil(t, customOp)
	})
}

func TestUpdateExpressions(t *testing.T) {
	t.Run("updateTimestamp creates correct expression", func(t *testing.T) {
		now := time.Now()
		expr := updateTimestamp(now)
		assert.NotNil(t, expr)
	})
}

func TestQueryStrategies(t *testing.T) {
	t.Run("ReverseLookupQuery sets up reverse index", func(t *testing.T) {
		table := New[TestData]("test-table")
		query := ReverseLookupQuery{
			QueryOptions: QueryOptions{
				PartitionKeyValue: "test-id",
				SortKeyPrefix:     "data#",
				Limit:             10,
			},
		}

		queryOp := table.Query(query)
		require.NotNil(t, queryOp)

		assert.Equal(t, "reverse-lookup-index", table.Options.ReverseLookupIndexName)
	})

	t.Run("CollectionQuery handles time bounds", func(t *testing.T) {
		table := New[TestData]("test-table")
		start := time.Now().Add(-time.Hour)
		end := time.Now()

		query := CollectionQuery{
			QueryOptions: QueryOptions{
				PartitionKeyValue: "test",
				CreatedAfter:      start,
				CreatedBefore:     end,
				Limit:             20,
			},
		}

		queryOp := table.Query(query)
		require.NotNil(t, queryOp)

		assert.Equal(t, "collection-query-index", table.Options.CollectionQueryIndexName)
	})

	t.Run("LookupQuery uses exact keys", func(t *testing.T) {
		table := New[TestData]("test-table")
		query := LookupQuery{
			QueryOptions: QueryOptions{
				PartitionKeyValue: "test#123",
				SortKeyPrefix:     "data#456",
			},
		}

		queryOp := table.Query(query)
		require.NotNil(t, queryOp)
	})

	t.Run("queries with invalid params panic", func(t *testing.T) {
		table := New[TestData]("test-table")

		assert.Panics(t, func() {
			table.Query(ReverseLookupQuery{})
		})

		assert.Panics(t, func() {
			table.Query(CollectionQuery{})
		})

		assert.Panics(t, func() {
			table.Query(LookupQuery{})
		})
	})
}
