package filter

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

type conditionEvaluator struct {
	condition expression.ConditionBuilder
}

type binaryConditionFunc = func(expression.OperandBuilder, expression.OperandBuilder) expression.ConditionBuilder

var comparableConditions = map[operation]binaryConditionFunc{
	OperationEqual:            expression.Equal,
	OperationNotEqual:         expression.NotEqual,
	OperationGreaterThan:      expression.GreaterThan,
	OperationGreaterThanEqual: expression.GreaterThanEqual,
	OperationLessThan:         expression.LessThan,
	OperationLessThanEqual:    expression.LessThanEqual,
}

func (c *conditionEvaluator) evaluateBinary(b *binaryCriteria) error {
	if b.operation == OperationBeginsWith {
		c.condition = expression.BeginsWith(
			expression.Name(b.attribute),
			b.value.(string),
		)
		return nil
	}
	if b.operation == OperationContains {
		c.condition = expression.Contains(
			expression.Name(b.attribute),
			expression.Value(b.value),
		)
		return nil
	}

	expr, ok := comparableConditions[b.operation]
	if !ok {
		return fmt.Errorf("unknown operation: %s", b.operation)
	}

	c.condition = expr(expression.Name(b.attribute), expression.Value(b.value))
	return nil
}

func (c *conditionEvaluator) evaluateUnary(u *unaryCriteria) error {
	return nil
}
