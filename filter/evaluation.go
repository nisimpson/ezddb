package filter

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

type conditionEvaluator struct {
	condition expression.ConditionBuilder
}

func Evaluate(e Expression) (expression.ConditionBuilder, error) {
	evaluator := conditionEvaluator{}
	err := e.evaluate(&evaluator)
	return evaluator.condition, err
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
	if b.operation == OperationIn {
		items := b.value.([]any)
		values := make([]expression.OperandBuilder, 0, len(items))
		for _, item := range items {
			values = append(values, expression.Value(item))
		}
		right := values[0]
		others := []expression.OperandBuilder{}
		if len(items) > 1 {
			others = values[1:]
		}
		c.condition = expression.In(
			expression.Name(b.attribute),
			right,
			others...,
		)
	}

	expr, ok := comparableConditions[b.operation]
	if !ok {
		return fmt.Errorf("%w: unsupported operation: %s", errors.ErrUnsupported, b)
	}

	c.condition = expr(expression.Name(b.attribute), expression.Value(b.value))
	return nil
}

func (c *conditionEvaluator) evaluateUnary(u *unaryCriteria) error {
	if u.operation == OperationExists {
		c.condition = expression.AttributeExists(
			expression.Name(u.attribute),
		)
		return nil
	}
	if u.operation == OperationNotExists {
		c.condition = expression.AttributeNotExists(
			expression.Name(u.attribute),
		)
		return nil
	}
	return fmt.Errorf("%w: operation: %s", errors.ErrUnsupported, u)
}

func (c *conditionEvaluator) evaluateBetween(b *betweenCriteria) error {
	c.condition = expression.Between(
		expression.Name(b.attribute),
		expression.Value(b.left),
		expression.Value(b.right),
	)
	return nil
}

func (c *conditionEvaluator) evaluateNot(n *notExpression) error {
	var (
		expr conditionEvaluator
		err  error
	)
	err = n.expr.evaluate(&expr)
	if err == nil {
		c.condition = expr.condition.Not()
		return nil
	}
	return err
}

func (c *conditionEvaluator) evaluateOr(o *orExpression) error {
	var (
		left     conditionEvaluator
		right    conditionEvaluator
		lefterr  error
		righterr error
	)
	lefterr = o.left.evaluate(&left)
	if lefterr == nil {
		righterr = o.right.evaluate(&right)
	}
	if righterr == nil {
		c.condition = expression.Or(left.condition, right.condition)
		return nil
	}
	return errors.Join(lefterr, righterr)
}

func (c *conditionEvaluator) evaluateAnd(a *andExpression) error {
	var (
		left     conditionEvaluator
		right    conditionEvaluator
		lefterr  error
		righterr error
	)
	lefterr = a.left.evaluate(&left)
	if lefterr == nil {
		righterr = a.right.evaluate(&right)
	}
	if righterr == nil {
		c.condition = expression.And(left.condition, right.condition)
		return nil
	}
	return errors.Join(lefterr, righterr)
}

type keyConditionEvaluator struct {
	condition expression.KeyConditionBuilder
}

func EvaluateKey(e Expression) (expression.KeyConditionBuilder, error) {
	evaluator := keyConditionEvaluator{}
	err := e.evaluate(&evaluator)
	return evaluator.condition, err
}

type binaryKeyConditionFunc = func(expression.KeyBuilder, expression.ValueBuilder) expression.KeyConditionBuilder

var comparableKeyConditions = map[operation]binaryKeyConditionFunc{
	OperationEqual:            expression.KeyEqual,
	OperationGreaterThan:      expression.KeyGreaterThan,
	OperationGreaterThanEqual: expression.KeyGreaterThanEqual,
	OperationLessThan:         expression.KeyLessThan,
	OperationLessThanEqual:    expression.KeyLessThanEqual,
}

func (c *keyConditionEvaluator) evaluateBinary(b *binaryCriteria) error {
	if b.operation == OperationBeginsWith {
		c.condition = expression.KeyBeginsWith(
			expression.Key(b.attribute),
			b.value.(string),
		)
		return nil
	}
	expr, ok := comparableKeyConditions[b.operation]
	if !ok {
		return fmt.Errorf("%w: unsupported operation: %s", errors.ErrUnsupported, b)
	}

	c.condition = expr(expression.Key(b.attribute), expression.Value(b.value))
	return nil
}

func (c *keyConditionEvaluator) evaluateUnary(u *unaryCriteria) error {
	return fmt.Errorf("%w: operation: %s", errors.ErrUnsupported, u)
}

func (c *keyConditionEvaluator) evaluateBetween(b *betweenCriteria) error {
	c.condition = expression.KeyBetween(
		expression.Key(b.attribute),
		expression.Value(b.left),
		expression.Value(b.right),
	)
	return nil
}

func (c *keyConditionEvaluator) evaluateNot(n *notExpression) error {
	return fmt.Errorf("%w: not operation", errors.ErrUnsupported)
}

func (c *keyConditionEvaluator) evaluateOr(o *orExpression) error {
	return fmt.Errorf("%w: or operation", errors.ErrUnsupported)
}

func (c *keyConditionEvaluator) evaluateAnd(a *andExpression) error {
	var (
		left     keyConditionEvaluator
		right    keyConditionEvaluator
		lefterr  error
		righterr error
	)
	lefterr = a.left.evaluate(&left)
	if lefterr == nil {
		righterr = a.right.evaluate(&right)
	}
	if righterr == nil {
		c.condition = expression.KeyAnd(left.condition, right.condition)
		return nil
	}
	return errors.Join(lefterr, righterr)
}
