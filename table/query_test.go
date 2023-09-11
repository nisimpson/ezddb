package table_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	mocks "github.com/nisimpson/ezddb/internal/mocks/github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestQuery(t *testing.T) {
	type testcase struct {
		name    string
		builder table.QueryBuilder
		want    dynamodb.QueryInput
		wantErr bool
	}

	for _, tc := range []testcase{
		{
			name:    "query partition",
			builder: table.New("table").Query("foo", table.KeyEquals(5)).Limit(100),
			want: dynamodb.QueryInput{
				TableName:              aws.String("table"),
				KeyConditionExpression: aws.String("#0 = :0"),
				Limit:                  aws.Int32(100),
				ExpressionAttributeNames: map[string]string{
					"#0": "foo",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberN{Value: "5"},
				},
				ScanIndexForward: aws.Bool(false),
			},
		},
		{

			name: "query partition from start key",
			builder: table.New("table").
				Query("foo", table.KeyEquals(5)).
				Limit(100).
				StartFromKey(ezddb.Item{
					"pk": &types.AttributeValueMemberS{Value: "foo"},
					"sk": &types.AttributeValueMemberS{Value: "bar"},
				}),
			want: dynamodb.QueryInput{
				TableName:              aws.String("table"),
				KeyConditionExpression: aws.String("#0 = :0"),
				Limit:                  aws.Int32(100),
				ExpressionAttributeNames: map[string]string{
					"#0": "foo",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberN{Value: "5"},
				},
				ExclusiveStartKey: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "foo"},
					"sk": &types.AttributeValueMemberS{Value: "bar"},
				},
				ScanIndexForward: aws.Bool(false),
			},
		},
		{

			name: "query partition from start token",
			builder: table.New("table", table.WithStartKeyProvider(
				mockPaginator{startKey: ezddb.Item{
					"pk": &types.AttributeValueMemberS{Value: "foo"},
					"sk": &types.AttributeValueMemberS{Value: "bar"},
				}},
			)).
				Query("foo", table.KeyEquals(5)).
				Limit(100).
				StartFromToken("token"),
			want: dynamodb.QueryInput{
				TableName:              aws.String("table"),
				KeyConditionExpression: aws.String("#0 = :0"),
				Limit:                  aws.Int32(100),
				ExpressionAttributeNames: map[string]string{
					"#0": "foo",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberN{Value: "5"},
				},
				ExclusiveStartKey: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "foo"},
					"sk": &types.AttributeValueMemberS{Value: "bar"},
				},
				ScanIndexForward: aws.Bool(false),
			},
		},
		{
			name:    "query and scan forward",
			builder: table.New("table").Query("foo", table.KeyEquals(5)).Forward(),
			want: dynamodb.QueryInput{
				TableName:              aws.String("table"),
				KeyConditionExpression: aws.String("#0 = :0"),
				ExpressionAttributeNames: map[string]string{
					"#0": "foo",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberN{Value: "5"},
				},
				ScanIndexForward: aws.Bool(true),
			},
		},
		{
			name:    "query index",
			builder: table.New("table").Query("foo", table.KeyEquals(5)).Index("gsi1"),
			want: dynamodb.QueryInput{
				TableName:              aws.String("table"),
				IndexName:              aws.String("gsi1"),
				KeyConditionExpression: aws.String("#0 = :0"),
				ExpressionAttributeNames: map[string]string{
					"#0": "foo",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberN{Value: "5"},
				},
				ScanIndexForward: aws.Bool(false),
			},
		},
		{
			name:    "query with range",
			builder: table.New("table").Query("foo", table.KeyEquals(5)).Range("bar", table.BeginsWith("baz")),
			want: dynamodb.QueryInput{
				TableName:              aws.String("table"),
				KeyConditionExpression: aws.String("(#0 = :0) AND (begins_with (#1, :1))"),
				ExpressionAttributeNames: map[string]string{
					"#0": "foo",
					"#1": "bar",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberN{Value: "5"},
					":1": &types.AttributeValueMemberS{Value: "baz"},
				},
				ScanIndexForward: aws.Bool(false),
			},
		},
		{
			name: "query with range and filter",
			builder: table.New("table").
				Query("foo", table.KeyEquals(5)).
				Range("bar", table.BeginsWith("baz")).
				Where("data.value", table.LessThan(7)).
				And("baz", table.Contains("string")).
				Or("duz", table.GreaterThanEqual(10)),
			want: dynamodb.QueryInput{
				TableName:              aws.String("table"),
				FilterExpression:       aws.String("((#0.#1 < :0) AND (contains (#2, :1))) OR (#3 >= :2)"),
				KeyConditionExpression: aws.String("(#4 = :3) AND (begins_with (#5, :4))"),
				ExpressionAttributeNames: map[string]string{
					"#0": "data",
					"#1": "value",
					"#2": "baz",
					"#3": "duz",
					"#4": "foo",
					"#5": "bar",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberN{Value: "7"},
					":1": &types.AttributeValueMemberS{Value: "string"},
					":2": &types.AttributeValueMemberN{Value: "10"},
					":3": &types.AttributeValueMemberN{Value: "5"},
					":4": &types.AttributeValueMemberS{Value: "baz"},
				},
				ScanIndexForward: aws.Bool(false),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("builds input", func(t *testing.T) {
				got, err := tc.builder.Build().Invoke(context.TODO())
				if tc.wantErr {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err)
				assert.EqualValues(t, &tc.want, got)
			})

			t.Run("executes", func(t *testing.T) {
				querier := mocks.NewMockQuerier(t)
				querier.On("Query", mock.Anything, mock.Anything).Return(&dynamodb.QueryOutput{}, nil)
				result := tc.builder.Execute(context.TODO(), querier)
				if tc.wantErr {
					assert.Error(t, result.Error())
				}
				assert.NoError(t, result.Error())
			})
		})
	}

	t.Run("build failed if the condition expression generates an error", func(t *testing.T) {
		builder := table.New("table", table.WithExpressionBuilder(failedExpression))
		_, err := builder.Query("pk", table.KeyEquals("foo")).Build().Invoke(context.TODO())
		assert.Error(t, err)
	})
}
