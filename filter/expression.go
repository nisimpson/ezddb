package filter

import (
	"fmt"
	"strings"
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

type binaryCriteria struct {
	attribute string
	operation operation
	value     any
}

func (c binaryCriteria) String() string {
	return fmt.Sprintf("('%s' %s '%v')", c.attribute, c.operation, c.value)
}

type unaryCriteria struct {
	attribute string
	operation operation
}

func (u unaryCriteria) String() string {
	return fmt.Sprintf("%s('%s')", u.attribute, u.operation)
}

type betweenCriteria struct {
	attribute string
	left      any
	right     any
}

func (b betweenCriteria) String() string {
	return fmt.Sprintf("between('%s','%v','%v')", b.attribute, b.left, b.right)
}

type attribute[T any] string

func AttributeOf[T any](key ...string) attribute[T] {
	path := strings.Join(key, ".")
	return attribute[T](path)
}

func Equals[T any, U any](a attribute[T], value U) Expression {
	return newBuilder(binaryCriteria{
		attribute: string(a),
		operation: OperationEqual,
		value:     value,
	})
}

func NotEquals[T any, U any](a attribute[T], value U) Expression {
	return newBuilder(binaryCriteria{
		attribute: string(a),
		operation: OperationNotEqual,
		value:     value,
	})
}

func LessThan[T any, U comparable](a attribute[T], value U) Expression {
	return newBuilder(binaryCriteria{
		attribute: string(a),
		operation: OperationLessThan,
		value:     value,
	})
}

func LessThanEqual[T any, U comparable](a attribute[T], value U) Expression {
	return newBuilder(binaryCriteria{
		attribute: string(a),
		operation: OperationLessThanEqual,
		value:     value,
	})
}

func GreaterThan[T any, U comparable](a attribute[T], value U) Expression {
	return newBuilder(binaryCriteria{
		attribute: string(a),
		operation: OperationGreaterThanEqual,
		value:     value,
	})
}

func GreaterThanEqual[T any, U comparable](a attribute[T], value U) Expression {
	return newBuilder(binaryCriteria{
		attribute: string(a),
		operation: OperationGreaterThanEqual,
		value:     value,
	})
}

func Exists[T any](a attribute[T]) Expression {
	return newBuilder(unaryCriteria{
		attribute: string(a),
		operation: OperationExists,
	})
}

func NotExists[T any](a attribute[T]) Expression {
	return newBuilder(unaryCriteria{
		attribute: string(a),
		operation: OperationNotExists,
	})
}

func HasSubstring[T any](a attribute[T], substr string) Expression {
	return newBuilder(binaryCriteria{
		attribute: string(a),
		operation: OperationContains,
		value:     substr,
	})
}

func Contains[T any, U any](a attribute[T], items ...U) Expression {
	var curr Builder
	for idx, item := range items {
		expr := newBuilder(binaryCriteria{
			attribute: string(a),
			operation: OperationContains,
			value:     item,
		})

		if idx == 0 {
			curr = expr
			continue
		}

		curr = curr.And(expr)
	}
	return curr
}

func Intersects[T any, U any](a attribute[T], items ...U) Expression {
	var curr Builder
	for idx, item := range items {
		expr := newBuilder(binaryCriteria{
			attribute: string(a),
			operation: OperationContains,
			value:     item,
		})

		if idx == 0 {
			curr = expr
			continue
		}

		curr = curr.Or(expr)
	}
	return curr
}

func HasPrefix[T any](a attribute[T], prefix string) Expression {
	return newBuilder(binaryCriteria{
		attribute: string(a),
		operation: OperationBeginsWith,
		value:     prefix,
	})
}

func IsOneOf[T any, U any](a attribute[T], items ...U) Expression {
	anyitems := make([]any, 0, len(items))
	for _, item := range items {
		anyitems = append(anyitems, item)
	}
	return newBuilder(binaryCriteria{
		attribute: string(a),
		operation: OperationIn,
		value:     anyitems,
	})
}

func IsBetween[T any, U comparable](a attribute[T], left U, right U) Expression {
	return newBuilder(betweenCriteria{
		attribute: string(a),
		left:      left,
		right:     right,
	})
}

type Expression interface {
	fmt.Stringer
	evaluate(Evaluator) error
}

type orExpression struct {
	left  Expression
	right Expression
}

func (o orExpression) String() string {
	return fmt.Sprintf("%s | %s", o.left, o.right)
}

type andExpression struct {
	left  Expression
	right Expression
}

func (a andExpression) String() string {
	return fmt.Sprintf("%s ^ %s", a.left, a.right)
}

type notExpression struct {
	expr Expression
}

func (n notExpression) String() string {
	return fmt.Sprintf("!(%s)", n.expr)
}

func (c binaryCriteria) evaluate(e Evaluator) error  { return e.evaluateBinary(&c) }
func (u unaryCriteria) evaluate(e Evaluator) error   { return e.evaluateUnary(&u) }
func (b betweenCriteria) evaluate(e Evaluator) error { return e.evaluateBetween(&b) }
func (o orExpression) evaluate(e Evaluator) error    { return e.evaluateOr(&o) }
func (a andExpression) evaluate(e Evaluator) error   { return e.evaluateAnd(&a) }
func (n notExpression) evaluate(e Evaluator) error   { return e.evaluateNot(&n) }

type Evaluator interface {
	evaluateBinary(*binaryCriteria) error
	evaluateOr(*orExpression) error
	evaluateAnd(*andExpression) error
	evaluateNot(*notExpression) error
	evaluateUnary(*unaryCriteria) error
	evaluateBetween(*betweenCriteria) error
}

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

func (b builder) Or(right Expression) Builder {
	return builder{expr: orExpression{left: b, right: right}}
}

func (b builder) Not() Builder {
	return builder{expr: b}
}

func newBuilder(e Expression) builder        { return builder{expr: e} }
func (b builder) evaluate(e Evaluator) error { return b.expr.evaluate(e) }
func (b builder) String() string             { return b.expr.String() }
