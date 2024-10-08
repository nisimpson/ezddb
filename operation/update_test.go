package operation_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/operation"
	"github.com/stretchr/testify/assert"
)

type updater struct {
	fixture
	dynamodb.UpdateItemOutput
	wantInput    *dynamodb.UpdateItemInput
	returnsError bool
}

func newUpdater(fixture fixture) updater {
	return updater{fixture: fixture}
}

func (p updater) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, options ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if p.returnsError {
		return nil, ErrMock
	} else if p.wantInput != nil && !assert.EqualValues(p.t, p.wantInput, input) {
		return nil, ErrAssertion
	} else {
		return &p.UpdateItemOutput, nil
	}
}

func (p updater) fails() updater {
	p.returnsError = true
	return p
}

func (t table) updateCustomer(c customer) operation.UpdateOperation {
	return func(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
		if t.OperationFails {
			return nil, ErrMock
		}
		return &dynamodb.UpdateItemInput{
			TableName: &t.tableName,
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: c.ID},
			},
			UpdateExpression: aws.String("SET #name = :name"),
			ExpressionAttributeNames: map[string]string{
				"name": *aws.String("name"),
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":name": &types.AttributeValueMemberS{Value: c.Name},
			},
		}, nil
	}
}

func TestUpdateInvoke(t *testing.T) {
	type testcase struct {
		name      string
		Operation operation.UpdateOperation
		wantInput dynamodb.UpdateItemInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			Operation: table.updateCustomer(customer{ID: "123", Name: "John Doe"}),
			wantInput: dynamodb.UpdateItemInput{
				TableName: aws.String("customer-table"),
				Key: map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: "123"},
				},
				UpdateExpression: aws.String("SET #name = :name"),
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
			Operation: table.failsTo().updateCustomer(customer{ID: "123", Name: "John Doe"}),
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

func TestUpdateExecute(t *testing.T) {
	type testcase struct {
		name      string
		updater   ezddb.Updater
		Operation operation.UpdateOperation
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the output successfully",
			Operation: table.updateCustomer(customer{ID: "123", Name: "John Doe"}),
			updater:   newUpdater(fixture{}),
			wantErr:   false,
		},
		{
			name:      "returns error if Operation fails",
			Operation: table.failsTo().updateCustomer(customer{ID: "123", Name: "John Doe"}),
			updater:   newUpdater(fixture{}),
			wantErr:   true,
		},
		{
			name:      "returns error if updater fails",
			Operation: table.updateCustomer(customer{ID: "123", Name: "John Doe"}),
			updater:   newUpdater(fixture{}).fails(),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := tc.Operation.Execute(context.TODO(), tc.updater)
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

func TestUpdateModify(t *testing.T) {
	type testcase struct {
		name      string
		Operation operation.UpdateOperation
		modifier  operation.UpdateModifier
		wantInput dynamodb.UpdateItemInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	modifier := operation.UpdateModifierFunc(func(ctx context.Context, input *dynamodb.UpdateItemInput) error {
		input.Key["modified"] = &types.AttributeValueMemberBOOL{Value: true}
		return nil
	})

	modifierFails := operation.UpdateModifierFunc(func(ctx context.Context, input *dynamodb.UpdateItemInput) error {
		return ErrMock
	})

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			Operation: table.updateCustomer(customer{ID: "123", Name: "John Doe"}),
			modifier:  modifier,
			wantInput: dynamodb.UpdateItemInput{
				TableName: aws.String("customer-table"),
				Key: map[string]types.AttributeValue{
					"id":       &types.AttributeValueMemberS{Value: "123"},
					"modified": &types.AttributeValueMemberBOOL{Value: true},
				},
				UpdateExpression: aws.String("SET #name = :name"),
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
			Operation: table.failsTo().updateCustomer(customer{ID: "123", Name: "John Doe"}),
			modifier:  modifierFails,
			wantErr:   true,
		},
		{
			name:      "returns error if modifier fails",
			Operation: table.updateCustomer(customer{ID: "123", Name: "John Doe"}),
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

func TestUpdateModifyTransactWriteItemInput(t *testing.T) {
	type testcase struct {
		name          string
		Operation     operation.UpdateOperation
		transactWrite dynamodb.TransactWriteItemsInput
		wantInput     dynamodb.TransactWriteItemsInput
		wantErr       bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			Operation: table.updateCustomer(customer{ID: "123", Name: "John Doe"}),
			wantInput: dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Update: &types.Update{
							TableName: aws.String("customer-table"),
							Key: map[string]types.AttributeValue{
								"id": &types.AttributeValueMemberS{Value: "123"},
							},
							UpdateExpression: aws.String("SET #name = :name"),
							ExpressionAttributeNames: map[string]string{
								"name": *aws.String("name"),
							},
							ExpressionAttributeValues: map[string]types.AttributeValue{
								":name": &types.AttributeValueMemberS{Value: "John Doe"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns error if invocation fails",
			Operation: table.failsTo().updateCustomer(customer{ID: "123", Name: "John Doe"}),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.Operation.ModifyTransactWriteItemsInput(context.TODO(), &tc.transactWrite)
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
