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

type transactGetter struct {
	fixture
	dynamodb.TransactGetItemsOutput
	wantInput    *dynamodb.TransactGetItemsInput
	returnsError bool
}

func newTransactGetter(fixture fixture) transactGetter {
	return transactGetter{fixture: fixture}
}

func (p transactGetter) TransactGetItems(ctx context.Context, input *dynamodb.TransactGetItemsInput, options ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	if p.returnsError {
		return nil, ErrMock
	} else if p.wantInput != nil && !assert.EqualValues(p.t, p.wantInput, input) {
		return nil, ErrAssertion
	} else {
		return &p.TransactGetItemsOutput, nil
	}
}

func (p transactGetter) fails() transactGetter {
	p.returnsError = true
	return p
}

func (t table) getCustomers(customers ...string) procedure.TransactionGetProcedure {
	transaction := procedure.NewTransactionGetProcedure()
	for _, c := range customers {
		transaction = transaction.Modify(t.getCustomer(c))
	}
	return transaction
}

func TestTransactionGetInvoke(t *testing.T) {
	type testcase struct {
		name      string
		procedure procedure.TransactionGetProcedure
		wantInput dynamodb.TransactGetItemsInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.getCustomers("123", "345"),
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
					{
						Get: &types.Get{
							TableName: aws.String("customer-table"),
							Key: map[string]types.AttributeValue{
								"id": &types.AttributeValueMemberS{Value: "345"},
							},
						},
					},
				},
			},
		},
		{
			name:      "returns error if procedure fails",
			procedure: table.failsTo().getCustomers("123", "345"),
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

func TestTransactionGetExecute(t *testing.T) {
	type testcase struct {
		name           string
		transactGetter procedure.TransactionGetter
		procedure      procedure.TransactionGetProcedure
		wantErr        bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:           "returns the output successfully",
			procedure:      table.getCustomers("123", "345"),
			transactGetter: newTransactGetter(fixture{}),
			wantErr:        false,
		},
		{
			name:           "returns error if procedure fails",
			procedure:      table.failsTo().getCustomers("123", "345"),
			transactGetter: newTransactGetter(fixture{}),
			wantErr:        true,
		},
		{
			name:           "returns error if transactGetter fails",
			procedure:      table.getCustomers("123", "345"),
			transactGetter: newTransactGetter(fixture{}).fails(),
			wantErr:        true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := tc.procedure.Execute(context.TODO(), tc.transactGetter)
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

func TestTransactionGetModify(t *testing.T) {
	type testcase struct {
		name      string
		procedure procedure.TransactionGetProcedure
		modifier  procedure.TransactionGetModifier
		wantInput dynamodb.TransactGetItemsInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	modifier := procedure.TransactionGetModifierFunc(func(ctx context.Context, input *dynamodb.TransactGetItemsInput) error {
		input.ReturnConsumedCapacity = types.ReturnConsumedCapacityTotal
		return nil
	})

	modifierFails := procedure.TransactionGetModifierFunc(func(ctx context.Context, input *dynamodb.TransactGetItemsInput) error {
		return ErrMock
	})

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.getCustomers("123", "345"),
			modifier:  modifier,
			wantInput: dynamodb.TransactGetItemsInput{
				ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
				TransactItems: []types.TransactGetItem{
					{
						Get: &types.Get{
							TableName: aws.String("customer-table"),
							Key: map[string]types.AttributeValue{
								"id": &types.AttributeValueMemberS{Value: "123"},
							},
						},
					},
					{
						Get: &types.Get{
							TableName: aws.String("customer-table"),
							Key: map[string]types.AttributeValue{
								"id": &types.AttributeValueMemberS{Value: "345"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns error if invocation fails",
			procedure: table.failsTo().getCustomers("123", "345"),
			modifier:  modifierFails,
			wantErr:   true,
		},
		{
			name:      "returns error if modifier fails",
			procedure: table.getCustomers("123", "345"),
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
