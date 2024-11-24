package stored_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/stored"
	"github.com/stretchr/testify/assert"
)

type querier struct {
	fixture
	dynamodb.QueryOutput
	wantInput    *dynamodb.QueryInput
	returnsError bool
}

func newQuerier(fixture fixture) querier {
	return querier{fixture: fixture}
}

func (p querier) Query(ctx context.Context, input *dynamodb.QueryInput, options ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if p.returnsError {
		return nil, ErrMock
	} else if p.wantInput != nil && !assert.EqualValues(p.t, p.wantInput, input) {
		return nil, ErrAssertion
	} else {
		return &p.QueryOutput, nil
	}
}

func (p querier) fails() querier {
	p.returnsError = true
	return p
}

func (t table) queryCustomerName(name string) stored.Query {
	return func(ctx context.Context) (*dynamodb.QueryInput, error) {
		if t.OperationFails {
			return nil, ErrMock
		}
		return &dynamodb.QueryInput{
			TableName:              &t.tableName,
			KeyConditionExpression: aws.String("#name = :name"),
			ExpressionAttributeNames: map[string]string{
				"name": *aws.String("name"),
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":name": &types.AttributeValueMemberS{Value: name},
			},
		}, nil
	}
}

func TestQueryInvoke(t *testing.T) {
	type testcase struct {
		name      string
		Operation stored.Query
		wantInput dynamodb.QueryInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			Operation: table.queryCustomerName("John Doe"),
			wantInput: dynamodb.QueryInput{
				TableName:              aws.String("customer-table"),
				KeyConditionExpression: aws.String("#name = :name"),
				ExpressionAttributeNames: map[string]string{
					"name": *aws.String("name"),
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":name": &types.AttributeValueMemberS{Value: "John Doe"},
				},
			},
		},
		{
			name:      "returns error if Operation fails",
			Operation: table.failsTo().queryCustomerName("John Doe"),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, err := tc.Operation.Invoke(context.TODO())
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			if !assert.NoError(t, err) {
				return
			}
			assert.EqualValues(t, &tc.wantInput, input)
		})
	}
}

func TestQueryExecute(t *testing.T) {
	type testcase struct {
		name      string
		querier   ezddb.Querier
		Operation stored.Query
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the output successfully",
			Operation: table.queryCustomerName("John Doe"),
			querier:   newQuerier(fixture{}),
			wantErr:   false,
		},
		{
			name:      "returns error if Operation fails",
			Operation: table.failsTo().queryCustomerName("John Doe"),
			querier:   newQuerier(fixture{}),
			wantErr:   true,
		},
		{
			name:      "returns error if querier fails",
			Operation: table.queryCustomerName("John Doe"),
			querier:   newQuerier(fixture{}).fails(),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := tc.Operation.Execute(context.TODO(), tc.querier)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			if !assert.NoError(t, err) {
				return
			}
			assert.NotNil(t, output)
		})
	}
}

func TestQueryModify(t *testing.T) {
	type testcase struct {
		name      string
		Operation stored.Query
		modifier  stored.QueryModifier
		wantInput dynamodb.QueryInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	modifier := stored.QueryModifierFunc(func(ctx context.Context, input *dynamodb.QueryInput) error {
		input.IndexName = aws.String("query-index")
		return nil
	})

	modifierFails := stored.QueryModifierFunc(func(ctx context.Context, input *dynamodb.QueryInput) error {
		return ErrMock
	})

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			Operation: table.queryCustomerName("John Doe"),
			modifier:  modifier,
			wantInput: dynamodb.QueryInput{
				IndexName:              aws.String("query-index"),
				TableName:              aws.String("customer-table"),
				KeyConditionExpression: aws.String("#name = :name"),
				ExpressionAttributeNames: map[string]string{
					"name": *aws.String("name"),
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":name": &types.AttributeValueMemberS{Value: "John Doe"},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns error if invocation fails",
			Operation: table.failsTo().queryCustomerName("John Doe"),
			modifier:  modifierFails,
			wantErr:   true,
		},
		{
			name:      "returns error if modifier fails",
			Operation: table.queryCustomerName("John Doe"),
			modifier:  modifierFails,
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, err := tc.Operation.Modify(tc.modifier).Invoke(context.TODO())
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			if !assert.NoError(t, err) {
				return
			}
			assert.EqualValues(t, &tc.wantInput, input)
		})
	}
}
