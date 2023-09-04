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

func (p PutBuilder) If(condition expression.ConditionBuilder) PutBuilder {
	p.conditionExpression = &condition
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
			expr, err = expression.NewBuilder().WithCondition(*p.conditionExpression).Build()
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
