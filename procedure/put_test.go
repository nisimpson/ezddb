package procedure_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb/procedure"
	"github.com/stretchr/testify/assert"
)

type putter struct {
	fixture
	dynamodb.PutItemOutput
	wantInput    *dynamodb.PutItemInput
	returnsError bool
}

func newPutter(fixture fixture) putter {
	return putter{fixture: fixture}
}

func (p putter) PutItem(ctx context.Context, input *dynamodb.PutItemInput, options ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if p.returnsError {
		return nil, ErrMock
	} else if p.wantInput != nil && !assert.EqualValues(p.t, p.wantInput, input) {
		return nil, ErrAssertion
	} else {
		return &p.PutItemOutput, nil
	}
}

func (p putter) fails() putter {
	p.returnsError = true
	return p
}

func (t table) putCustomer(c customer) procedure.PutProcedure {
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		if t.procedureFails {
			return nil, ErrMock
		}
		item := must(attributevalue.MarshalMap(c))
		return &dynamodb.PutItemInput{
			TableName: &t.tableName,
			Item:      item,
		}, nil
	}
}

func TestPutInvoke(t *testing.T) {
	type testcase struct {
		name      string
		procedure procedure.PutProcedure
		wantInput dynamodb.PutItemInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.putCustomer(customer{ID: "123", Name: "John Doe"}),
			wantInput: dynamodb.PutItemInput{
				TableName: aws.String("customer-table"),
				Item: map[string]types.AttributeValue{
					"id":   &types.AttributeValueMemberS{Value: "123"},
					"name": &types.AttributeValueMemberS{Value: "John Doe"},
				},
			},
		},
		{
			name:      "returns error if procedure fails",
			procedure: table.failsTo().putCustomer(customer{ID: "123", Name: "John Doe"}),
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

func TestPutExecute(t *testing.T) {
	type testcase struct {
		name      string
		putter    procedure.Putter
		procedure procedure.PutProcedure
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the output successfully",
			procedure: table.putCustomer(customer{ID: "123", Name: "John Doe"}),
			putter:    newPutter(fixture{}),
			wantErr:   false,
		},
		{
			name:      "returns error if procedure fails",
			procedure: table.failsTo().putCustomer(customer{ID: "123", Name: "John Doe"}),
			putter:    newPutter(fixture{}),
			wantErr:   true,
		},
		{
			name:      "returns error if putter fails",
			procedure: table.putCustomer(customer{ID: "123", Name: "John Doe"}),
			putter:    newPutter(fixture{}).fails(),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := tc.procedure.Execute(context.TODO(), tc.putter)
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

func TestPutModify(t *testing.T) {
	type testcase struct {
		name      string
		procedure procedure.PutProcedure
		modifier  procedure.PutModifier
		wantInput dynamodb.PutItemInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	modifier := procedure.PutModifierFunc(func(ctx context.Context, input *dynamodb.PutItemInput) error {
		input.Item["modified"] = &types.AttributeValueMemberBOOL{Value: true}
		return nil
	})

	modifierFails := procedure.PutModifierFunc(func(ctx context.Context, input *dynamodb.PutItemInput) error {
		return ErrMock
	})

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.putCustomer(customer{ID: "123", Name: "John Doe"}),
			modifier:  modifier,
			wantInput: dynamodb.PutItemInput{
				TableName: aws.String("customer-table"),
				Item: map[string]types.AttributeValue{
					"id":       &types.AttributeValueMemberS{Value: "123"},
					"name":     &types.AttributeValueMemberS{Value: "John Doe"},
					"modified": &types.AttributeValueMemberBOOL{Value: true},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns error if invocation fails",
			procedure: table.failsTo().putCustomer(customer{ID: "123", Name: "John Doe"}),
			modifier:  modifierFails,
			wantErr:   true,
		},
		{
			name:      "returns error if modifier fails",
			procedure: table.putCustomer(customer{ID: "123", Name: "John Doe"}),
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

func TestPutModifyBatchWriteItemInput(t *testing.T) {
	type testcase struct {
		name       string
		procedure  procedure.PutProcedure
		batchwrite dynamodb.BatchWriteItemInput
		wantInput  dynamodb.BatchWriteItemInput
		wantErr    bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.putCustomer(customer{ID: "123", Name: "John Doe"}),
			wantInput: dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"customer-table": {
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"id":   &types.AttributeValueMemberS{Value: "123"},
									"name": &types.AttributeValueMemberS{Value: "John Doe"},
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
			procedure: table.putCustomer(customer{ID: "123", Name: "John Doe"}),
			batchwrite: dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"customer-table": {},
				},
			},
			wantInput: dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"customer-table": {
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"id":   &types.AttributeValueMemberS{Value: "123"},
									"name": &types.AttributeValueMemberS{Value: "John Doe"},
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
			procedure: table.failsTo().putCustomer(customer{ID: "123", Name: "John Doe"}),
			wantErr:   true,
		},
		{
			name: "returns error if table name is missing",
			procedure: table.putCustomer(customer{ID: "123", Name: "John Doe"}).Modify(
				procedure.PutModifierFunc(
					func(ctx context.Context, input *dynamodb.PutItemInput) error {
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

func TestPutModifyTransactWriteItemInput(t *testing.T) {
	type testcase struct {
		name          string
		procedure     procedure.PutProcedure
		transactWrite dynamodb.TransactWriteItemsInput
		wantInput     dynamodb.TransactWriteItemsInput
		wantErr       bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.putCustomer(customer{ID: "123", Name: "John Doe"}),
			wantInput: dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Put: &types.Put{
							TableName: aws.String("customer-table"),
							Item: map[string]types.AttributeValue{
								"id":   &types.AttributeValueMemberS{Value: "123"},
								"name": &types.AttributeValueMemberS{Value: "John Doe"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns error if invocation fails",
			procedure: table.failsTo().putCustomer(customer{ID: "123", Name: "John Doe"}),
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
