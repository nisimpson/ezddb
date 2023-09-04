package procedure_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb/procedure"
	"github.com/stretchr/testify/assert"
)

type deleter struct {
	fixture
	dynamodb.DeleteItemOutput
	wantInput    *dynamodb.DeleteItemInput
	returnsError bool
}

func newDeleter(fixture fixture) deleter {
	return deleter{fixture: fixture}
}

func (p deleter) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, options ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if p.returnsError {
		return nil, ErrMock
	} else if p.wantInput != nil && !assert.EqualValues(p.t, p.wantInput, input) {
		return nil, ErrAssertion
	} else {
		return &p.DeleteItemOutput, nil
	}
}

func (p deleter) fails() deleter {
	p.returnsError = true
	return p
}

func (t table) deleteCustomer(id string) procedure.Delete {
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		if t.procedureFails {
			return nil, ErrMock
		}
		return &dynamodb.DeleteItemInput{
			TableName: &t.tableName,
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "123"},
			},
		}, nil
	}
}

func TestDeleteInvoke(t *testing.T) {
	type testcase struct {
		name      string
		procedure procedure.Delete
		wantInput dynamodb.DeleteItemInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.deleteCustomer("123"),
			wantInput: dynamodb.DeleteItemInput{
				TableName: aws.String("customer-table"),
				Key: map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: "123"},
				},
			},
		},
		{
			name:      "returns error if procedure fails",
			procedure: table.failsTo().deleteCustomer("123"),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, err := tc.procedure.Invoke(context.TODO())
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

func TestDeleteExecute(t *testing.T) {
	type testcase struct {
		name      string
		deleter   procedure.Deleter
		procedure procedure.Delete
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the output successfully",
			procedure: table.deleteCustomer("123"),
			deleter:   newDeleter(fixture{}),
			wantErr:   false,
		},
		{
			name:      "returns error if procedure fails",
			procedure: table.failsTo().deleteCustomer("123"),
			deleter:   newDeleter(fixture{}),
			wantErr:   true,
		},
		{
			name:      "returns error if deleter fails",
			procedure: table.deleteCustomer("123"),
			deleter:   newDeleter(fixture{}).fails(),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := tc.procedure.Execute(context.TODO(), tc.deleter)
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

func TestDeleteModify(t *testing.T) {
	type testcase struct {
		name      string
		procedure procedure.Delete
		modifier  procedure.DeleteModifier
		wantInput dynamodb.DeleteItemInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	modifier := procedure.DeleteModifierFunc(func(ctx context.Context, input *dynamodb.DeleteItemInput) error {
		input.ReturnConsumedCapacity = types.ReturnConsumedCapacityTotal
		return nil
	})

	modifierFails := procedure.DeleteModifierFunc(func(ctx context.Context, input *dynamodb.DeleteItemInput) error {
		return ErrMock
	})

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.deleteCustomer("123"),
			modifier:  modifier,
			wantInput: dynamodb.DeleteItemInput{
				ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
				TableName:              aws.String("customer-table"),
				Key: map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: "123"},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns error if invocation fails",
			procedure: table.failsTo().deleteCustomer("123"),
			modifier:  modifierFails,
			wantErr:   true,
		},
		{
			name:      "returns error if modifier fails",
			procedure: table.deleteCustomer("123"),
			modifier:  modifierFails,
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, err := tc.procedure.Modify(tc.modifier).Invoke(context.TODO())
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

func TestDeleteModifyBatchWriteItemInput(t *testing.T) {
	type testcase struct {
		name       string
		procedure  procedure.Delete
		batchwrite dynamodb.BatchWriteItemInput
		wantInput  dynamodb.BatchWriteItemInput
		wantErr    bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.deleteCustomer("123"),
			wantInput: dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"customer-table": {
						{
							DeleteRequest: &types.DeleteRequest{
								Key: map[string]types.AttributeValue{
									"id": &types.AttributeValueMemberS{Value: "123"},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns the input when the input is non empty",
			procedure: table.deleteCustomer("123"),
			batchwrite: dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"customer-table": {},
				},
			},
			wantInput: dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"customer-table": {
						{
							DeleteRequest: &types.DeleteRequest{
								Key: map[string]types.AttributeValue{
									"id": &types.AttributeValueMemberS{Value: "123"},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns error if invocation fails",
			procedure: table.failsTo().deleteCustomer("123"),
			wantErr:   true,
		},
		{
			name: "returns error if table name is missing",
			procedure: table.deleteCustomer("123").Modify(
				procedure.DeleteModifierFunc(
					func(ctx context.Context, input *dynamodb.DeleteItemInput) error {
						input.TableName = nil
						return nil
					},
				),
			),
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.procedure.ModifyBatchWriteItemInput(context.TODO(), &tc.batchwrite)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			if !assert.NoError(t, err) {
				return
			}
			assert.EqualValues(t, tc.wantInput, tc.batchwrite)
		})
	}
}

func TestDeleteModifyTransactWriteItemInput(t *testing.T) {
	type testcase struct {
		name          string
		procedure     procedure.Delete
		transactWrite dynamodb.TransactWriteItemsInput
		wantInput     dynamodb.TransactWriteItemsInput
		wantErr       bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.deleteCustomer("123"),
			wantInput: dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Delete: &types.Delete{
							TableName: aws.String("customer-table"),
							Key: map[string]types.AttributeValue{
								"id": &types.AttributeValueMemberS{Value: "123"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns error if invocation fails",
			procedure: table.failsTo().deleteCustomer("123"),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.procedure.ModifyTransactWriteItemsInput(context.TODO(), &tc.transactWrite)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			if !assert.NoError(t, err) {
				return
			}
			assert.EqualValues(t, tc.wantInput, tc.transactWrite)
		})
	}
}
