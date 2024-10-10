package filter

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

type operation string

const (
	OperationEqual            operation = "="
	OperationNotEqual         operation = "<>"
	OperationLessThan         operation = "<"
	OperationLessThanEqual    operation = "<="
	OperationGreaterThan      operation = ">"
	OperationGreaterThanEqual operation = ">="
	OperationContains         operation = "contains"
	OperationExists           operation = "exists"
	OperationNotExists        operation = "!exists"
	OperationBeginsWith       operation = "begins_with"
	OperationIn               operation = "in"
)

type Attribute interface {
	name() string
}

type attribute string

func AttributeOf(key ...string) Attribute {
	path := strings.Join(key, ".")
	return attribute(path)
}

func (a attribute) name() string { return string(a) }

func Equals[U any](a Attribute, value U) Builder {
	return newBuilder(binaryCriteria{
		Attribute: a.name(),
		Operation: OperationEqual,
		Value:     value,
	})
}

func NotEquals[U any](a Attribute, value U) Builder {
	return newBuilder(binaryCriteria{
		Attribute: a.name(),
		Operation: OperationNotEqual,
		Value:     value,
	})
}

func LessThan[U comparable](a Attribute, value U) Builder {
	return newBuilder(binaryCriteria{
		Attribute: a.name(),
		Operation: OperationLessThan,
		Value:     value,
	})
}

func LessThanEqual[U comparable](a Attribute, value U) Builder {
	return newBuilder(binaryCriteria{
		Attribute: a.name(),
		Operation: OperationLessThanEqual,
		Value:     value,
	})
}

func GreaterThan[U comparable](a Attribute, value U) Builder {
	return newBuilder(binaryCriteria{
		Attribute: a.name(),
		Operation: OperationGreaterThanEqual,
		Value:     value,
	})
}

func GreaterThanEqual[U comparable](a Attribute, value U) Builder {
	return newBuilder(binaryCriteria{
		Attribute: a.name(),
		Operation: OperationGreaterThanEqual,
		Value:     value,
	})
}

func Exists(a attribute) Builder {
	return newBuilder(unaryCriteria{
		Attribute: a.name(),
		Operation: OperationExists,
	})
}

func NotExists(a attribute) Builder {
	return newBuilder(unaryCriteria{
		Attribute: a.name(),
		Operation: OperationNotExists,
	})
}

func HasSubstring(a Attribute, substr string) Builder {
	return newBuilder(binaryCriteria{
		Attribute: a.name(),
		Operation: OperationContains,
		Value:     substr,
	})
}

func Contains[U any](a Attribute, items ...U) Builder {
	var curr Builder
	for idx, item := range items {
		expr := newBuilder(binaryCriteria{
			Attribute: a.name(),
			Operation: OperationContains,
			Value:     item,
		})

		if idx == 0 {
			curr = expr
			continue
		}

		curr = curr.And(expr)
	}
	return curr
}

func Intersects[U any](a Attribute, items ...U) Builder {
	var curr Builder
	for idx, item := range items {
		expr := newBuilder(binaryCriteria{
			Attribute: a.name(),
			Operation: OperationContains,
			Value:     item,
		})

		if idx == 0 {
			curr = expr
			continue
		}

		curr = curr.Or(expr)
	}
	return curr
}

func HasPrefix(a Attribute, prefix string) Builder {
	return newBuilder(binaryCriteria{
		Attribute: a.name(),
		Operation: OperationBeginsWith,
		Value:     prefix,
	})
}

func IsOneOf[U any](a Attribute, items ...U) Builder {
	anyitems := make([]any, 0, len(items))
	for _, item := range items {
		anyitems = append(anyitems, item)
	}
	return newBuilder(binaryCriteria{
		Attribute: a.name(),
		Operation: OperationIn,
		Value:     anyitems,
	})
}

func IsBetween[U comparable](a Attribute, left U, right U) Builder {
	return newBuilder(betweenCriteria{
		Attribute: a.name(),
		Left:      left,
		Right:     right,
	})
}

func TimestampEquals(a Attribute, value time.Time) Builder {
	return Equals(a, value.Format(time.RFC3339))
}

func TimestampBetween(a Attribute, start, end time.Time) Builder {
	return IsBetween(a, start.Format(time.RFC3339), end.Format(time.RFC3339))
}

func ExpiresAfter(a Attribute, dt time.Time) Builder {
	return LessThanEqual(a, dt.Unix())
}

type Expression interface {
	fmt.Stringer
	evaluate(inspector) error
	condition() expression.ConditionBuilder
	keyCondition() expression.KeyConditionBuilder
}

func Condition(e Expression) expression.ConditionBuilder {
	return e.condition()
}

func KeyCondition(e Expression) expression.KeyConditionBuilder {
	return e.keyCondition()
}

type binaryCriteria struct {
	Attribute string
	Operation operation
	Value     any
}

func (b binaryCriteria) String() string {
	return fmt.Sprintf("('%s' %s '%v')", b.Attribute, b.Operation, b.Value)
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

func (b binaryCriteria) condition() (cb expression.ConditionBuilder) {
	if b.Operation == OperationBeginsWith {
		return expression.BeginsWith(
			expression.Name(b.Attribute),
			b.Value.(string),
		)
	}
	if b.Operation == OperationContains {
		return expression.Contains(
			expression.Name(b.Attribute),
			expression.Value(b.Value),
		)
	}
	if b.Operation == OperationIn {
		items := b.Value.([]any)
		values := make([]expression.OperandBuilder, 0, len(items))
		for _, item := range items {
			values = append(values, expression.Value(item))
		}
		right := values[0]
		others := []expression.OperandBuilder{}
		if len(items) > 1 {
			others = values[1:]
		}
		return expression.In(
			expression.Name(b.Attribute),
			right,
			others...,
		)
	}

	expr, ok := comparableConditions[b.Operation]
	if !ok {
		return expr(expression.Name(b.Attribute), expression.Value(b.Value))
	}

	return
}

type binaryKeyConditionFunc = func(expression.KeyBuilder, expression.ValueBuilder) expression.KeyConditionBuilder

var comparableKeyConditions = map[operation]binaryKeyConditionFunc{
	OperationEqual:            expression.KeyEqual,
	OperationGreaterThan:      expression.KeyGreaterThan,
	OperationGreaterThanEqual: expression.KeyGreaterThanEqual,
	OperationLessThan:         expression.KeyLessThan,
	OperationLessThanEqual:    expression.KeyLessThanEqual,
}

func (b binaryCriteria) keyCondition() (cb expression.KeyConditionBuilder) {
	if b.Operation == OperationBeginsWith {
		return expression.KeyBeginsWith(
			expression.Key(b.Attribute),
			b.Value.(string),
		)
	}
	expr, ok := comparableKeyConditions[b.Operation]
	if !ok {
		return expr(expression.Key(b.Attribute), expression.Value(b.Value))
	}

	return
}

type unaryCriteria struct {
	Attribute string
	Operation operation
}

func (u unaryCriteria) String() string {
	return fmt.Sprintf("%s('%s')", u.Attribute, u.Operation)
}

func (u unaryCriteria) condition() (cb expression.ConditionBuilder) {
	if u.Operation == OperationExists {
		return expression.AttributeExists(expression.Name(u.Attribute))
	}
	if u.Operation == OperationNotExists {
		return expression.AttributeNotExists(expression.Name(u.Attribute))
	}
	return
}

func (u unaryCriteria) keyCondition() (cb expression.KeyConditionBuilder) { return }

type betweenCriteria struct {
	Attribute string
	Left      any
	Right     any
}

func (b betweenCriteria) String() string {
	return fmt.Sprintf("between('%s','%v','%v')", b.Attribute, b.Left, b.Right)
}

func (b betweenCriteria) condition() (cb expression.ConditionBuilder) {
	return expression.Between(
		expression.Name(b.Attribute),
		expression.Value(b.Left),
		expression.Value(b.Right),
	)
}

func (b betweenCriteria) keyCondition() (cb expression.KeyConditionBuilder) {
	return expression.KeyBetween(
		expression.Key(b.Attribute),
		expression.Value(b.Left),
		expression.Value(b.Right),
	)
}

type orExpression struct {
	left  Expression
	right Expression
}

func (o orExpression) String() string {
	return fmt.Sprintf("%s | %s", o.left, o.right)
}

func (o orExpression) condition() (cb expression.ConditionBuilder) {
	return o.left.condition().Or(o.right.condition())
}

func (o orExpression) keyCondition() (cb expression.KeyConditionBuilder) { return }

type andExpression struct {
	left  Expression
	right Expression
}

func (a andExpression) String() string {
	return fmt.Sprintf("%s ^ %s", a.left, a.right)
}

func (a andExpression) condition() (cb expression.ConditionBuilder) {
	return a.left.condition().And(a.right.condition())
}

func (a andExpression) keyCondition() (cb expression.KeyConditionBuilder) {
	return a.left.keyCondition().And(a.right.keyCondition())
}

type notExpression struct {
	expr Expression
}

func (n notExpression) String() string {
	return fmt.Sprintf("!(%s)", n.expr)
}

func (n notExpression) condition() (cb expression.ConditionBuilder) {
	return n.expr.condition().Not()
}

func (n notExpression) keyCondition() (cb expression.KeyConditionBuilder) { return }

type Builder interface {
	Expression
	And(Expression) Builder
	Or(Expression) Builder
	Not() Builder
}

type builder struct {
	expr Expression
}

func (b builder) And(right Expression) Builder {
	return builder{expr: andExpression{left: b, right: right}}
}

func And(left Expression, right Expression) Builder {
	return newBuilder(left).And(right)
}

func (b builder) Or(right Expression) Builder {
	return builder{expr: orExpression{left: b, right: right}}
}

func Or(left Expression, right Expression) Builder {
	return newBuilder(left).Or(right)
}

func (b builder) Not() Builder {
	return builder{expr: b}
}

func Not(expr Expression) Builder {
	return newBuilder(expr).Not()
}

func newBuilder(e Expression) builder                          { return builder{expr: e} }
func (b builder) evaluate(i inspector) error                   { return b.expr.evaluate(i) }
func (b builder) condition() expression.ConditionBuilder       { return b.expr.condition() }
func (b builder) keyCondition() expression.KeyConditionBuilder { return b.expr.keyCondition() }
func (b builder) String() string                               { return b.expr.String() }

type InspectToken int

const (
	StartToken InspectToken = iota
	EndToken
	BinaryToken
	UnaryToken
	BetweenToken
	AndToken
	OrToken
	NotToken
)

type Variant struct {
	Binary  *binaryCriteria
	Unary   *unaryCriteria
	Between *betweenCriteria
	And     *andExpression
	Or      *orExpression
	Not     *notExpression
}

type inspector struct {
	inspect func(InspectToken, Variant) error
}

func Inspect(e Expression, fn func(InspectToken, Variant) error) error {
	inspector := inspector{inspect: fn}
	return errors.Join(
		fn(StartToken, Variant{}),
		e.evaluate(inspector),
		fn(EndToken, Variant{}),
	)
}

func (c binaryCriteria) evaluate(i inspector) error  { return i.evaluateBinary(&c) }
func (u unaryCriteria) evaluate(i inspector) error   { return i.evaluateUnary(&u) }
func (b betweenCriteria) evaluate(i inspector) error { return i.evaluateBetween(&b) }
func (o orExpression) evaluate(i inspector) error    { return i.evaluateOr(&o) }
func (a andExpression) evaluate(i inspector) error   { return i.evaluateAnd(&a) }
func (n notExpression) evaluate(i inspector) error   { return i.evaluateNot(&n) }

func (i inspector) evaluateBinary(b *binaryCriteria) error {
	return i.inspect(BinaryToken, Variant{Binary: b})
}

func (i inspector) evaluateUnary(u *unaryCriteria) error {
	return i.inspect(UnaryToken, Variant{Unary: u})
}

func (i inspector) evaluateBetween(b *betweenCriteria) error {
	return i.inspect(BetweenToken, Variant{Between: b})
}

func (i inspector) evaluateOr(o *orExpression) error {
	return errors.Join(
		o.left.evaluate(i),
		i.inspect(OrToken, Variant{Or: o}),
		o.right.evaluate(i),
	)
}

func (i inspector) evaluateAnd(a *andExpression) error {
	return errors.Join(
		a.left.evaluate(i),
		i.inspect(AndToken, Variant{And: a}),
		a.right.evaluate(i),
	)
}

func (i inspector) evaluateNot(n *notExpression) error {
	return errors.Join(
		n.expr.evaluate(i),
		i.inspect(NotToken, Variant{Not: n}),
	)
}
