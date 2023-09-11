package table_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	mocks "github.com/nisimpson/ezddb/internal/mocks/github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPut(t *testing.T) {
	type testcase struct {
		name    string
		builder table.PutBuilder
		data    any
		want    dynamodb.PutItemInput
		wantErr bool
	}

	stringMap := map[string]string{"foo": "bar", "baz": "duz"}

	for _, tc := range []testcase{
		{
			name:    "empty put builder",
			data:    stringMap,
			builder: table.New("table").Put(stringMap),
			want: dynamodb.PutItemInput{
				TableName: aws.String("table"),
				Item: map[string]types.AttributeValue{
					"foo": &types.AttributeValueMemberS{Value: "bar"},
					"baz": &types.AttributeValueMemberS{Value: "duz"},
				},
			},
		},
		{
			name: "put if attribute exists",
			data: stringMap,
			builder: table.New("table").
				Put(stringMap).
				If("foo", table.Exists()),
			want: dynamodb.PutItemInput{
				TableName: aws.String("table"),
				Item: map[string]types.AttributeValue{
					"foo": &types.AttributeValueMemberS{Value: "bar"},
					"baz": &types.AttributeValueMemberS{Value: "duz"},
				},
				ConditionExpression: aws.String("attribute_exists (#0)"),
				ExpressionAttributeNames: map[string]string{
					"#0": "foo",
				},
			},
		},
		{
			name: "put if attribute exists and equals",
			data: stringMap,
			builder: table.New("table").
				Put(stringMap).
				If("foo", table.Exists()).
				And("baz", table.Equals("something else")),
			want: dynamodb.PutItemInput{
				TableName: aws.String("table"),
				Item: map[string]types.AttributeValue{
					"foo": &types.AttributeValueMemberS{Value: "bar"},
					"baz": &types.AttributeValueMemberS{Value: "duz"},
				},
				ConditionExpression: aws.String("(attribute_exists (#0)) AND (#1 = :0)"),
				ExpressionAttributeNames: map[string]string{
					"#0": "foo",
					"#1": "baz",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberS{Value: "something else"},
				},
			},
		},
		{
			name: "put if attribute exists and value not equals",
			data: stringMap,
			builder: table.New("table").
				Put(stringMap).
				If("foo", table.Exists()).
				And("baz", table.NotEquals("something else")),
			want: dynamodb.PutItemInput{
				TableName: aws.String("table"),
				Item: map[string]types.AttributeValue{
					"foo": &types.AttributeValueMemberS{Value: "bar"},
					"baz": &types.AttributeValueMemberS{Value: "duz"},
				},
				ConditionExpression: aws.String("(attribute_exists (#0)) AND (#1 <> :0)"),
				ExpressionAttributeNames: map[string]string{
					"#0": "foo",
					"#1": "baz",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberS{Value: "something else"},
				},
			},
		},
		{
			name: "put if attribute exists or value not between 'aaa' and 'bbb'",
			data: stringMap,
			builder: table.New("table").
				Put(stringMap).
				If("foo", table.Exists()).
				Or("baz", table.Not(table.Between("aaa", "bbb"))),
			want: dynamodb.PutItemInput{
				TableName: aws.String("table"),
				Item: map[string]types.AttributeValue{
					"foo": &types.AttributeValueMemberS{Value: "bar"},
					"baz": &types.AttributeValueMemberS{Value: "duz"},
				},
				ConditionExpression: aws.String("(attribute_exists (#0)) OR (NOT (#1 BETWEEN :0 AND :1))"),
				ExpressionAttributeNames: map[string]string{
					"#0": "foo",
					"#1": "baz",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberS{Value: "aaa"},
					":1": &types.AttributeValueMemberS{Value: "bbb"},
				},
			},
		},
		{
			name: "sets a custom filter condition",
			data: stringMap,
			builder: table.New("table").Put(stringMap).WithCondition(
				expression.AttributeExists(expression.Name("bar")),
			),
			want: dynamodb.PutItemInput{
				TableName: aws.String("table"),
				Item: map[string]types.AttributeValue{
					"foo": &types.AttributeValueMemberS{Value: "bar"},
					"baz": &types.AttributeValueMemberS{Value: "duz"},
				},
				ConditionExpression: aws.String("attribute_exists (#0)"),
				ExpressionAttributeNames: map[string]string{
					"#0": "bar",
				},
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
				putter := mocks.NewMockPutter(t)
				putter.On("PutItem", mock.Anything, mock.Anything).Return(&dynamodb.PutItemOutput{}, nil)
				err := tc.builder.Execute(context.TODO(), putter)
				if tc.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})
		})
	}

	t.Run("build fails if the item cannot be marshaled", func(t *testing.T) {
		builder := table.New("table", table.WithMapMarshaler(failedMarshaler))
		_, err := builder.Put(stringMap).Build().Invoke(context.TODO())
		assert.Error(t, err)
	})

	t.Run("build failed if the condition expression generates an error", func(t *testing.T) {
		builder := table.New("table", table.WithExpressionBuilder(failedExpression))
		_, err := builder.Put(stringMap).If("attribute", table.Exists()).Build().Invoke(context.TODO())
		assert.Error(t, err)
	})
}
