package table

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb/internal/xslices"
	"github.com/nisimpson/ezddb/procedure"
)

const (
	MaxTransactWriteItems = 100
	MaxTransactReadItems  = 100
)

type TransactWriteBuilder struct {
	modifiers []procedure.TransactionWriteModifier
}

func TransactWrite(writers ...procedure.TransactionWriteModifier) TransactWriteBuilder {
	builder := TransactWriteBuilder{modifiers: writers}
	return builder
}

func (t TransactWriteBuilder) If(tableName string, attribute string, expr FilterExpression) TransactWriteBuilder {
	t.modifiers = append(t.modifiers,
		procedure.TransactionWriteModifierFunc(func(ctx context.Context,
			twii *dynamodb.TransactWriteItemsInput) error {
			return nil
		}))
	return t
}

func (t TransactWriteBuilder) Build() procedure.MultiTransactionWrite {
	chunks := xslices.Chunk(t.modifiers, MaxTransactWriteItems)
	procs := make([]procedure.TransactionWrite, 0, len(chunks))
	for _, chunk := range chunks {
		proc := procedure.NewTransactionWriteProcedure()
		proc = proc.Modify(chunk...)
		procs = append(procs, proc)
	}
	return procs
}

type TransactGetBuilder struct {
	modifiers []procedure.TransactionGetModifier
}

func TransactGet(readers ...procedure.TransactionGetModifier) TransactGetBuilder {
	builder := TransactGetBuilder{modifiers: readers}
	return builder
}

func (t TransactGetBuilder) Build() procedure.MultiTransactionGet {
	chunks := xslices.Chunk(t.modifiers, MaxTransactWriteItems)
	procs := make([]procedure.TransactionGet, 0, len(chunks))
	for _, chunk := range chunks {
		proc := procedure.NewTransactionGetProcedure()
		proc = proc.Modify(chunk...)
		procs = append(procs, proc)
	}
	return procs
}

type ConditionCheckBuilder struct {
	builder   Builder
	key       ItemKeyProvider
	condition expression.ConditionBuilder
}

func (c ConditionCheckBuilder) If(attribute string, eval FilterExpression) ConditionCheckBuilder {
	var cond expression.ConditionBuilder
	if c.condition.IsSet() {
		cond = c.condition.And(eval.filter(attribute))
	} else {
		cond = eval.filter(attribute)
	}
	c.condition = cond
	return c
}

func (c ConditionCheckBuilder) And(attribute string, eval FilterExpression) ConditionCheckBuilder {
	return c.If(attribute, eval)
}

func (c ConditionCheckBuilder) Or(attribute string, eval FilterExpression) ConditionCheckBuilder {
	var cond expression.ConditionBuilder
	if c.condition.IsSet() {
		cond = c.condition.Or(eval.filter(attribute))
	} else {
		cond = eval.filter(attribute)
	}
	c.condition = cond
	return c
}

func (c ConditionCheckBuilder) OrNot(attribute string, eval FilterExpression) ConditionCheckBuilder {
	var cond expression.ConditionBuilder
	if c.condition.IsSet() {
		cond = c.condition.Or(eval.filter(attribute).Not())
	} else {
		cond = eval.filter(attribute).Not()
	}
	c.condition = cond
	return c
}

func (c ConditionCheckBuilder) AndNot(attribute string, eval FilterExpression) ConditionCheckBuilder {
	var cond expression.ConditionBuilder
	if c.condition.IsSet() {
		cond = c.condition.And(eval.filter(attribute).Not())
	} else {
		cond = eval.filter(attribute).Not()
	}
	c.condition = cond
	return c
}

func (c ConditionCheckBuilder) ModifyTransactWriteItemsInput(ctx context.Context,
	input *dynamodb.TransactWriteItemsInput) error {
	if !c.condition.IsSet() {
		return nil
	}
	expr, err := expression.NewBuilder().WithCondition(c.condition).Build()
	if err != nil {
		return err
	}
	input.TransactItems = append(input.TransactItems, types.TransactWriteItem{
		ConditionCheck: &types.ConditionCheck{
			TableName:                 &c.builder.tableName,
			Key:                       c.key.DynamoItemKey(),
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		},
	})
	return nil
}
