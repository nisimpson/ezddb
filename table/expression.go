package table

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/nisimpson/ezddb/internal/xslices"
)

type number interface {
	int | int64 | int32 | int16 | int8 | uint | uint8 | uint16 | uint32 | uint64
}

type keytype interface {
	number | string
}

type KeyExpression func(attribute string) expression.KeyConditionBuilder

func KeyEquals[T keytype](value T) KeyExpression {
	return func(attribute string) expression.KeyConditionBuilder {
		return expression.KeyEqual(expression.Key(attribute), expression.Value(value))
	}
}

func KeyBeginsWith(value string) KeyExpression {
	return func(attribute string) expression.KeyConditionBuilder {
		return expression.KeyBeginsWith(expression.Key(attribute), value)
	}
}

func KeyLessThan[T keytype](value T) KeyExpression {
	return func(attribute string) expression.KeyConditionBuilder {
		return expression.KeyLessThan(expression.Key(attribute), expression.Value(value))
	}
}

func KeyLessThanEqual[T keytype](value T) KeyExpression {
	return func(attribute string) expression.KeyConditionBuilder {
		return expression.KeyLessThanEqual(expression.Key(attribute), expression.Value(value))
	}
}

func KeyGreaterThan[T keytype](value T) KeyExpression {
	return func(attribute string) expression.KeyConditionBuilder {
		return expression.KeyGreaterThan(expression.Key(attribute), expression.Value(value))
	}
}

func KeyGreaterThanEqual[T keytype](value T) KeyExpression {
	return func(attribute string) expression.KeyConditionBuilder {
		return expression.KeyGreaterThanEqual(expression.Key(attribute), expression.Value(value))
	}
}

func KeyBetween[T keytype](lower, upper T) KeyExpression {
	return func(attribute string) expression.KeyConditionBuilder {
		return expression.KeyBetween(expression.Key(attribute),
			expression.Value(lower),
			expression.Value(upper))
	}
}

type FilterExpression interface {
	filter(attribute string) expression.ConditionBuilder
}

type SizeExpression interface {
	size(attribute string) expression.ConditionBuilder
}

type FilterExpressionFunc func(attribute string) expression.ConditionBuilder

func (fn FilterExpressionFunc) filter(attribute string) expression.ConditionBuilder {
	return fn(attribute)
}

func AttributeNotExists() FilterExpressionFunc {
	return func(attribute string) expression.ConditionBuilder {
		return expression.AttributeNotExists(expression.Name(attribute))
	}
}

func AttributeExists() FilterExpressionFunc {
	return func(attribute string) expression.ConditionBuilder {
		return expression.AttributeExists(expression.Name(attribute))
	}
}

type EqualsExpression struct {
	value any
}

func (e EqualsExpression) filter(attribute string) expression.ConditionBuilder {
	return expression.Equal(expression.Name(attribute), expression.Value(e.value))
}

func (e EqualsExpression) size(attribute string) expression.ConditionBuilder {
	return expression.Size(expression.Name(attribute)).Equal(expression.Value(e.value))
}

func Equals(value any) EqualsExpression {
	return EqualsExpression{value: value}
}

type NotEqualsExpression struct {
	value any
}

func (n NotEqualsExpression) filter(attribute string) expression.ConditionBuilder {
	return expression.NotEqual(expression.Name(attribute), expression.Value(n.value))
}

func (n NotEqualsExpression) size(attribute string) expression.ConditionBuilder {
	return expression.Size(expression.Name(attribute)).NotEqual(expression.Value(n.value))
}

func NotEquals(value any) NotEqualsExpression {
	return NotEqualsExpression{value: value}
}

func BeginsWith(value string) FilterExpressionFunc {
	return func(attribute string) expression.ConditionBuilder {
		return expression.BeginsWith(expression.Name(attribute), value)
	}
}

type LessThanExpression[T keytype] struct {
	value T
}

func (l LessThanExpression[T]) filter(attribute string) expression.ConditionBuilder {
	return expression.LessThan(expression.Name(attribute), expression.Value(l.value))
}

func (l LessThanExpression[T]) size(attribute string) expression.ConditionBuilder {
	return expression.Size(
		expression.Name(attribute)).LessThan(expression.Value(l.value))
}

func LessThan[T keytype](value T) FilterExpressionFunc {
	return func(attribute string) expression.ConditionBuilder {
		return expression.LessThan(expression.Name(attribute), expression.Value(value))
	}
}

type LessThanEqualExpression[T keytype] struct {
	value T
}

func (l LessThanEqualExpression[T]) filter(attribute string) expression.ConditionBuilder {
	return expression.LessThanEqual(expression.Name(attribute), expression.Value(l.value))
}

func (l LessThanEqualExpression[T]) size(attribute string) expression.ConditionBuilder {
	return expression.
		Size(expression.Name(attribute)).
		LessThanEqual(expression.Value(l.value))
}

func LessThanEqual[T keytype](value T) LessThanEqualExpression[T] {
	return LessThanEqualExpression[T]{value: value}
}

type GreaterThanExpression[T keytype] struct {
	value T
}

func (g GreaterThanExpression[T]) filter(attribute string) expression.ConditionBuilder {
	return expression.GreaterThan(expression.Name(attribute), expression.Value(g.value))
}

func (g GreaterThanExpression[T]) size(attribute string) expression.ConditionBuilder {
	return expression.Size(
		expression.Name(attribute)).GreaterThan(expression.Value(g.value))
}

func GreaterThan[T keytype](value T) GreaterThanExpression[T] {
	return GreaterThanExpression[T]{value: value}
}

type GreaterThanEqualExpression[T keytype] struct {
	value T
}

func (g GreaterThanEqualExpression[T]) filter(attribute string) expression.ConditionBuilder {
	return expression.GreaterThanEqual(expression.Name(attribute), expression.Value(g.value))
}

func (g GreaterThanEqualExpression[T]) size(attribute string) expression.ConditionBuilder {
	return expression.Size(
		expression.Name(attribute)).GreaterThanEqual(expression.Value(g.value))
}

func GreaterThanEqual[T keytype](value T) GreaterThanEqualExpression[T] {
	return GreaterThanEqualExpression[T]{value: value}
}

type BetweenExpression[T keytype] struct {
	lower T
	upper T
}

func (b BetweenExpression[T]) filter(attribute string) expression.ConditionBuilder {
	return expression.Between(expression.Name(attribute),
		expression.Value(b.lower),
		expression.Value(b.upper))
}

func (b BetweenExpression[T]) size(attribute string) expression.ConditionBuilder {
	return expression.Size(
		expression.Name(attribute)).Between(
		expression.Value(b.lower),
		expression.Value(b.upper))
}

func Between[T keytype](lower, upper T) BetweenExpression[T] {
	return BetweenExpression[T]{upper: upper, lower: lower}
}

type InExpression struct {
	values []any
}

func (b InExpression) filter(attribute string) expression.ConditionBuilder {
	if len(b.values) == 1 {
		return expression.In(expression.Name(attribute), expression.Value(b.values[0]))
	}
	return expression.In(
		expression.Name(attribute),
		expression.Value(b.values[0]),
		xslices.MapSlice(b.values[1:], func(value any) expression.OperandBuilder {
			return expression.Value(value)
		})...,
	)
}

func (b InExpression) size(attribute string) expression.ConditionBuilder {
	if len(b.values) == 1 {
		return expression.Size(expression.Name(attribute)).In(expression.Value(b.values[0]))
	}
	return expression.Size(
		expression.Name(attribute)).In(
		expression.Value(b.values[0]),
		xslices.MapSlice(b.values[1:], func(value any) expression.OperandBuilder {
			return expression.Value(value)
		})...,
	)
}

func In(values ...any) InExpression {
	return InExpression{values: values}
}

func IsType(value expression.DynamoDBAttributeType) FilterExpressionFunc {
	return func(attribute string) expression.ConditionBuilder {
		return expression.AttributeType(expression.Name(attribute), value)
	}
}

func Contains(value string) FilterExpressionFunc {
	return func(attribute string) expression.ConditionBuilder {
		return expression.Contains(expression.Name(attribute), value)
	}
}

func ContainsSubstring(value string) FilterExpressionFunc {
	return Contains(value)
}

func ContainsElement(value string) FilterExpressionFunc {
	return Contains(value)
}
