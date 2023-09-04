package table

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/procedure"
)

type DeleteBuilder struct {
	builder          Builder
	key              ItemKeyProvider
	conditionBuilder *expression.ConditionBuilder
}

func (d DeleteBuilder) If(condition expression.ConditionBuilder) DeleteBuilder {
	d.conditionBuilder = &condition
	return d
}

func (d DeleteBuilder) Build() procedure.Delete {
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		var expr expression.Expression
		var err error

		if d.conditionBuilder != emptyConditionBuilder {
			expr, err = expression.NewBuilder().WithCondition(*d.conditionBuilder).Build()
			if err != nil {
				return nil, err
			}
		}

		return &dynamodb.DeleteItemInput{
			TableName:                 &d.builder.tableName,
			Key:                       d.key.DynamoItemKey(),
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		}, nil
	}
}

func (d DeleteBuilder) Execute(ctx context.Context, deleter ezddb.Deleter) error {
	_, err := d.Build().Execute(ctx, deleter)
	return err
}
