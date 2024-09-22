package ezddb_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/stretchr/testify/assert"
)

type getter struct {
	fixture
	dynamodb.GetItemOutput
	wantInput    *dynamodb.GetItemInput
	returnsError bool
}

func newgetter(fixture fixture) getter {
	return getter{fixture: fixture}
}

func (p getter) GetItem(ctx context.Context, input *dynamodb.GetItemInput, options ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if p.returnsError {
		return nil, ErrMock
	} else if p.wantInput != nil && !assert.EqualValues(p.t, p.wantInput, input) {
		return nil, ErrAssertion
	} else {
		return &p.GetItemOutput, nil
	}
}

func (p getter) fails() getter {
	p.returnsError = true
	return p
}

func (t table) getCustomer(id string) ezddb.GetProcedure {
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		if t.procedureFails {
			return nil, ErrMock
		}
		return &dynamodb.GetItemInput{
			TableName: &t.tableName,
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: id},
			},
		}, nil
	}
}

func TestGetInvoke(t *testing.T) {
	type testcase struct {
		name      string
		procedure ezddb.GetProcedure
		wantInput dynamodb.GetItemInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.getCustomer("123"),
			wantInput: dynamodb.GetItemInput{
				TableName: aws.String("customer-table"),
				Key: map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: "123"},
				},
			},
		},
		{
			name:      "returns error if procedure fails",
			procedure: table.failsTo().getCustomer("123"),
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

func TestGetExecute(t *testing.T) {
	type testcase struct {
		name      string
		getter    ezddb.Getter
		procedure ezddb.GetProcedure
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the output successfully",
			procedure: table.getCustomer("123"),
			getter:    newgetter(fixture{}),
			wantErr:   false,
		},
		{
			name:      "returns error if procedure fails",
			procedure: table.failsTo().getCustomer("123"),
			getter:    newgetter(fixture{}),
			wantErr:   true,
		},
		{
			name:      "returns error if getter fails",
			procedure: table.getCustomer("123"),
			getter:    newgetter(fixture{}).fails(),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := tc.procedure.Execute(context.TODO(), tc.getter)
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

func TestGetModify(t *testing.T) {
	type testcase struct {
		name      string
		procedure ezddb.GetProcedure
		modifier  ezddb.GetModifier
		wantInput dynamodb.GetItemInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	modifier := ezddb.GetModifierFunc(func(ctx context.Context, input *dynamodb.GetItemInput) error {
		input.Key["modified"] = &types.AttributeValueMemberBOOL{Value: true}
		return nil
	})

	modifierFails := ezddb.GetModifierFunc(func(ctx context.Context, input *dynamodb.GetItemInput) error {
		return ErrMock
	})

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.getCustomer("123"),
			modifier:  modifier,
			wantInput: dynamodb.GetItemInput{
				TableName: aws.String("customer-table"),
				Key: map[string]types.AttributeValue{
					"id":       &types.AttributeValueMemberS{Value: "123"},
					"modified": &types.AttributeValueMemberBOOL{Value: true},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns error if invocation fails",
			procedure: table.failsTo().getCustomer("123"),
			modifier:  modifierFails,
			wantErr:   true,
		},
		{
			name:      "returns error if modifier fails",
			procedure: table.getCustomer("123"),
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

func TestGetModifyBatchGetItemInput(t *testing.T) {
	type testcase struct {
		name      string
		procedure ezddb.GetProcedure
		batchget  dynamodb.BatchGetItemInput
		wantInput dynamodb.BatchGetItemInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.getCustomer("123"),
			wantInput: dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					"customer-table": {
						Keys: []map[string]types.AttributeValue{
							{
								"id": &types.AttributeValueMemberS{Value: "123"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns the input when the input is non empty",
			procedure: table.getCustomer("123"),
			batchget: dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					"customer-table": {},
				},
			},
			wantInput: dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					"customer-table": {
						Keys: []map[string]types.AttributeValue{
							{
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
			procedure: table.failsTo().getCustomer("123"),
			wantErr:   true,
		},
		{
			name: "returns error if table name is missing",
			procedure: table.getCustomer("123").Modify(
				ezddb.GetModifierFunc(
					func(ctx context.Context, input *dynamodb.GetItemInput) error {
						input.TableName = nil
						return nil
					},
				),
			),
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.procedure.ModifyBatchGetItemInput(context.TODO(), &tc.batchget)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			if !assert.NoError(t, err) {
				return
			}
			assert.EqualValues(t, tc.wantInput, tc.batchget)
		})
	}
}

func TestGetModifyTransactGetItemInput(t *testing.T) {
	type testcase struct {
		name        string
		procedure   ezddb.GetProcedure
		transactGet dynamodb.TransactGetItemsInput
		wantInput   dynamodb.TransactGetItemsInput
		wantErr     bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.getCustomer("123"),
			wantInput: dynamodb.TransactGetItemsInput{
				TransactItems: []types.TransactGetItem{
					{
						Get: &types.Get{
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
			procedure: table.failsTo().getCustomer("123"),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.procedure.ModifyTransactGetItemsInput(context.TODO(), &tc.transactGet)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			if !assert.NoError(t, err) {
				return
			}
			assert.EqualValues(t, tc.wantInput, tc.transactGet)
		})
	}
}
