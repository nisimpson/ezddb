package table

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb/procedure"
)

type UpdateBuilder struct {
	builder          Builder
	key              ItemKeyProvider
	updateBuilder    *expression.UpdateBuilder
	conditionBuilder *expression.ConditionBuilder
}

func (u UpdateBuilder) WithUpdateExpression(builder expression.UpdateBuilder) UpdateBuilder {
	u.updateBuilder = &builder
	return u
}

func (u UpdateBuilder) If(builder expression.ConditionBuilder) UpdateBuilder {
	u.conditionBuilder = &builder
	return u
}

func (u UpdateBuilder) Then() UpdateBuilder {
	return u
}

func (u UpdateBuilder) Build() procedure.Update {
	return func(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
		builder := expression.NewBuilder().WithUpdate(*u.updateBuilder)
		if u.conditionBuilder != emptyConditionBuilder {
			builder = builder.WithCondition(*u.conditionBuilder)
		}
		expr, err := builder.Build()

		if err != nil {
			return nil, err
		}

		return &dynamodb.UpdateItemInput{
			TableName:                 &u.builder.tableName,
			Key:                       u.key.DynamoItemKey(),
			UpdateExpression:          expr.Update(),
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		}, nil
	}
}

func (u UpdateBuilder) Increment(attribute string, addable Addable) UpdateBuilder {
	var add expression.UpdateBuilder
	if u.isEmptyUpdate() {
		add = expression.Add(expression.Name(attribute), addable.toExpressionValue())
	} else {
		add = u.updateBuilder.Add(expression.Name(attribute), addable.toExpressionValue())
	}
	u.updateBuilder = &add
	return u
}

func (u UpdateBuilder) AddElement(attribute string, value any) UpdateBuilder {
	var add expression.UpdateBuilder
	if u.isEmptyUpdate() {
		add = expression.Add(expression.Name(attribute), expression.Value(value))
	} else {
		add = expression.Add(expression.Name(attribute), expression.Value(value))
	}
	u.updateBuilder = &add
	return u
}

func (u UpdateBuilder) DeleteElements(attribute string, values ...any) UpdateBuilder {
	var del expression.UpdateBuilder
	if u.isEmptyUpdate() {
		del = expression.Delete(expression.Name(attribute), expression.Value(values))
	} else {
		del = expression.Delete(expression.Name(attribute), expression.Value(values))
	}
	u.updateBuilder = &del
	return u
}

func (u UpdateBuilder) Remove(attribute string) UpdateBuilder {
	var remove expression.UpdateBuilder
	if u.isEmptyUpdate() {
		remove = expression.Remove(expression.Name(attribute))
	} else {
		remove = u.updateBuilder.Remove(expression.Name(attribute))
	}
	u.updateBuilder = &remove
	return u
}

func (u UpdateBuilder) RemoveElementsAtIndex(attribute string, indices ...int) UpdateBuilder {
	var names expression.NameBuilder
	for i, value := range indices {
		name := expression.Name(fmt.Sprintf("%s[%d]", attribute, value))
		if i == 0 {
			names = name
			continue
		}
		names = names.AppendName(name)
	}
	var remove expression.UpdateBuilder
	if u.isEmptyUpdate() {
		remove = expression.Remove(names)
	} else {
		remove = u.updateBuilder.Remove(names)
	}
	u.updateBuilder = &remove
	return u
}

func (u UpdateBuilder) Set(attribute string, value any) UpdateBuilder {
	nameExpr := expression.Name(attribute)
	valueExpr := expression.Value(value)

	if u.isEmptyUpdate() {
		set := expression.Set(nameExpr, valueExpr)
		u.updateBuilder = &set
	} else {
		set := u.updateBuilder.Set(nameExpr, valueExpr)
		u.updateBuilder = &set
	}
	return u
}

func (u UpdateBuilder) SetIf(attribute string) UpdateSetBuilder {
	return UpdateSetBuilder{
		builder: u,
		name:    expression.Name(attribute),
		operand: func(value any) expression.OperandBuilder {
			return expression.Value(value)
		},
	}
}

func (u UpdateBuilder) isEmptyUpdate() bool {
	return u.updateBuilder == emptyUpdateBuilder
}

type UpdateSetBuilder struct {
	builder UpdateBuilder
	name    expression.NameBuilder
	operand func(value any) expression.OperandBuilder
}

func (u UpdateSetBuilder) NotExists() UpdateSetBuilder {
	u.operand = func(value any) expression.OperandBuilder {
		return expression.IfNotExists(u.name, expression.Value(value))
	}
	return u
}

func (u UpdateSetBuilder) AttributeNotExists(attribute string) UpdateSetBuilder {
	u.operand = func(value any) expression.OperandBuilder {
		return expression.IfNotExists(expression.Name(attribute), expression.Value(value))
	}
	return u
}

func (u UpdateSetBuilder) Value(value any) UpdateBuilder {
	if u.builder.isEmptyUpdate() {
		set := expression.Set(u.name, u.operand(value))
		u.builder.updateBuilder = &set
	} else {
		set := u.builder.updateBuilder.Set(u.name, u.operand(value))
		u.builder.updateBuilder = &set
	}
	return u.builder
}

type Addable interface {
	toExpressionValue() expression.ValueBuilder
}

type AddableFunc func() expression.ValueBuilder

func Number[T number](value T) AddableFunc {
	return func() expression.ValueBuilder {
		return expression.Value(value)
	}
}
