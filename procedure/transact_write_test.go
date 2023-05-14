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

type transactWriter struct {
	fixture
	dynamodb.TransactWriteItemsOutput
	wantInput    *dynamodb.TransactWriteItemsInput
	returnsError bool
}

func newTransactWriter(fixture fixture) transactWriter {
	return transactWriter{fixture: fixture}
}

func (p transactWriter) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, options ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if p.returnsError {
		return nil, ErrMock
	} else if p.wantInput != nil && !assert.EqualValues(p.t, p.wantInput, input) {
		return nil, ErrAssertion
	} else {
		return &p.TransactWriteItemsOutput, nil
	}
}

func (p transactWriter) fails() transactWriter {
	p.returnsError = true
	return p
}

func (t table) updateCustomers(customers ...customer) procedure.TransactionWriteProcedure {
	transaction := procedure.NewTransactionWriteProcedure()
	for _, c := range customers {
		transaction = transaction.Modify(t.updateCustomer(c))
	}
	return transaction
}

func TestTransactWriteInvoke(t *testing.T) {
	type testcase struct {
		name      string
		procedure procedure.TransactionWriteProcedure
		wantInput dynamodb.TransactWriteItemsInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.updateCustomers(
				customer{ID: "123", Name: "John Doe"},
				customer{ID: "345", Name: "Jane Doe"},
			),
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
					{
						Update: &types.Update{
							TableName: aws.String("customer-table"),
							Key: map[string]types.AttributeValue{
								"id": &types.AttributeValueMemberS{Value: "345"},
							},
							UpdateExpression: aws.String("SET #name = :name"),
							ExpressionAttributeNames: map[string]string{
								"name": *aws.String("name"),
							},
							ExpressionAttributeValues: map[string]types.AttributeValue{
								":name": &types.AttributeValueMemberS{Value: "Jane Doe"},
							},
						},
					},
				},
			},
		},
		{
			name:      "returns error if procedure fails",
			procedure: table.failsTo().updateCustomers(customer{ID: "123", Name: "John Doe"}),
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

func TestTransactWriteExecute(t *testing.T) {
	type testcase struct {
		name      string
		transactWriter   procedure.TransactionWriter
		procedure procedure.TransactionWriteProcedure
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	for _, tc := range []testcase{
		{
			name:      "returns the output successfully",
			procedure: table.updateCustomers(customer{ID: "123", Name: "John Doe"}),
			transactWriter:   newTransactWriter(fixture{}),
			wantErr:   false,
		},
		{
			name:      "returns error if procedure fails",
			procedure: table.failsTo().updateCustomers(customer{ID: "123", Name: "John Doe"}),
			transactWriter:   newTransactWriter(fixture{}),
			wantErr:   true,
		},
		{
			name:      "returns error if transactWriter fails",
			procedure: table.updateCustomers(customer{ID: "123", Name: "John Doe"}),
			transactWriter:   newTransactWriter(fixture{}).fails(),
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := tc.procedure.Execute(context.TODO(), tc.transactWriter)
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

func TestTransactWriteModify(t *testing.T) {
	type testcase struct {
		name      string
		procedure procedure.TransactionWriteProcedure
		modifier  procedure.TransactionWriteModifier
		wantInput dynamodb.TransactWriteItemsInput
		wantErr   bool
	}

	table := table{tableName: "customer-table"}

	modifier := procedure.TransactionWriteModifierFunc(func(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
		input.ClientRequestToken = aws.String("token")
		return nil
	})

	modifierFails := procedure.TransactionWriteModifierFunc(func(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
		return ErrMock
	})

	for _, tc := range []testcase{
		{
			name:      "returns the input successfully",
			procedure: table.updateCustomers(
				customer{ID: "123", Name: "John Doe"},
				customer{ID: "345", Name: "Jane Doe"},
			),
			modifier:  modifier,
			wantInput: dynamodb.TransactWriteItemsInput{
				ClientRequestToken: aws.String("token"),
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
					{
						Update: &types.Update{
							TableName: aws.String("customer-table"),
							Key: map[string]types.AttributeValue{
								"id": &types.AttributeValueMemberS{Value: "345"},
							},
							UpdateExpression: aws.String("SET #name = :name"),
							ExpressionAttributeNames: map[string]string{
								"name": *aws.String("name"),
							},
							ExpressionAttributeValues: map[string]types.AttributeValue{
								":name": &types.AttributeValueMemberS{Value: "Jane Doe"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "returns error if invocation fails",
			procedure: table.failsTo().updateCustomers(customer{ID: "123", Name: "John Doe"}),
			modifier:  modifierFails,
			wantErr:   true,
		},
		{
			name:      "returns error if modifier fails",
			procedure: table.updateCustomers(customer{ID: "123", Name: "John Doe"}),
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
