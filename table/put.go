package table

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/procedure"
)

type PutBuilder struct {
	data                any
	conditionExpression *expression.ConditionBuilder
	builder             Builder
}

func (b Builder) Put(data any) PutBuilder {
	return PutBuilder{
		builder:             b,
		data:                data,
		conditionExpression: emptyConditionBuilder,
	}
}

func (p PutBuilder) WithCondition(expr expression.ConditionBuilder) PutBuilder {
	p.conditionExpression = &expr
	return p
}

func (p PutBuilder) If(attribute string, expr FilterExpression) PutBuilder {
	var cond expression.ConditionBuilder = expr.filter(attribute)
	p.conditionExpression = &cond
	return p
}

func (p PutBuilder) And(attribute string, expr FilterExpression) PutBuilder {
	var cond = p.conditionExpression.And(expr.filter(attribute))
	p.conditionExpression = &cond
	return p
}

func (p PutBuilder) Or(attribute string, expr FilterExpression) PutBuilder {
	var cond expression.ConditionBuilder = p.conditionExpression.Or(expr.filter(attribute))
	p.conditionExpression = &cond
	return p
}

func (p PutBuilder) Build() procedure.Put {
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		item, err := p.builder.marshalMap(p.data)
		if err != nil {
			return nil, err
		}

		var expr expression.Expression

		if p.conditionExpression != emptyConditionBuilder {
			builder := expression.NewBuilder().WithCondition(*p.conditionExpression)
			expr, err = p.builder.buildExpression(builder)
			if err != nil {
				return nil, err
			}
		}

		return &dynamodb.PutItemInput{
			TableName:                 &p.builder.tableName,
			Item:                      item,
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		}, nil
	}
}

func (p PutBuilder) Execute(ctx context.Context, putter ezddb.Putter) error {
	_, err := p.Build().Execute(ctx, putter)
	return err
}
