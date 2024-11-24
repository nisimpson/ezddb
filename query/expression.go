package query

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

// operation represents the type of comparison or check to be performed in a query expression
type operation string

const (
	OperationEqual            operation = "="           // OperationEqual represents an equality comparison (=)
	OperationNotEqual         operation = "<>"          // OperationNotEqual represents an inequality comparison (<>)
	OperationLessThan         operation = "<"           // OperationLessThan represents a less than comparison (<)
	OperationLessThanEqual    operation = "<="          // OperationLessThanEqual represents a less than or equal comparison (<=)
	OperationGreaterThan      operation = ">"           // OperationGreaterThan represents a greater than comparison (>)
	OperationGreaterThanEqual operation = ">="          // OperationGreaterThanEqual represents a greater than or equal comparison (>=)
	OperationContains         operation = "contains"    // OperationContains represents a contains check for strings or sets
	OperationExists           operation = "exists"      // OperationExists checks if an attribute exists
	OperationNotExists        operation = "!exists"     // OperationNotExists checks if an attribute does not exist
	OperationBeginsWith       operation = "begins_with" // OperationBeginsWith checks if a string attribute begins with a given substring
	OperationIn               operation = "in"          // OperationIn checks if a value is in a set of values
	OperationTrue             operation = "true"        // OperationTrue represents a constant true condition
)

// ItemAttribute represents a field name in a DynamoDB table item that can be used in queries and conditions.
// It provides methods for building various types of query expressions and conditions. The literal value is
// used as the attribute name, and follows the same conventions as the [expression.NameBuilder].
type ItemAttribute string

// name returns the attribute name as a string representation.
func (a ItemAttribute) name() string { return string(a) }

// KeyBuilder creates an AWS SDK V2 [expression.KeyBuilder] for this attribute for use in key condition expressions.
func (a ItemAttribute) KeyBuilder() expression.KeyBuilder { return expression.Key(a.name()) }

// NameBuilder creates an AWS SDK V2 [expression.NameBuilder] for this attribute for use in various DynamoDB expressions
func (a ItemAttribute) NameBuilder() expression.NameBuilder { return expression.Name(a.name()) }

// Equals creates an expression that checks if the attribute equals the given value.
func (a ItemAttribute) Equals(value any) Builder { return Equals(a, value) }

// NotEquals creates an expression that checks if the attribute does not equal the given value.
func (a ItemAttribute) NotEquals(value any) Builder { return NotEquals(a, value) }

// LessThan creates an expression that checks if the attribute is less than the given value.
func (a ItemAttribute) LessThan(value any) Builder { return LessThan(a, value) }

// LessThanEqual creates an expression that checks if the attribute is less than or equal to the given value.
func (a ItemAttribute) LessThanEqual(value any) Builder { return LessThanEqual(a, value) }

// GreaterThan creates an expression that checks if the attribute is greater than the given value.
func (a ItemAttribute) GreaterThan(value any) Builder { return GreaterThan(a, value) }

// GreaterThanEqual creates an expression that checks if the attribute is greater than or equal to the given value.
func (a ItemAttribute) GreaterThanEqual(value any) Builder { return GreaterThanEqual(a, value) }

// Exists creates a condition checking if this attribute exists in the item.
func (a ItemAttribute) Exists() Builder { return Exists(a) }

// NotExists creates a condition checking if this attribute does not exist in the item.
func (a ItemAttribute) NotExists() Builder { return NotExists(a) }

// HasSubstring creates a condition checking if this string attribute contains the given substring.
func (a ItemAttribute) HasSubstring(substr string) Builder { return HasSubstring(a, substr) }

// Contains creates a condition checking if this set attribute contains any of the provided items.
// The attribute's value should be a list or set.
func (a ItemAttribute) Contains(items ...any) Builder { return Contains(a, items...) }

// Intersects creates a condition checking if this set attribute has any elements in common with the provided items.
// The attribute value should be a list or a set.
func (a ItemAttribute) Intersects(items ...any) Builder { return Intersects(a, items...) }

// HasPrefix creates a condition checking if this string attribute begins with the given prefix.
func (a ItemAttribute) HasPrefix(substr string) Builder { return HasPrefix(a, substr) }

// IsOneOf creates a condition checking if this attribute's value matches any of the provided values.
func (a ItemAttribute) IsOneOf(items ...any) Builder { return IsOneOf(a, items...) }

// IsBetween creates a condition checking if this attribute's value falls between the start and end values.
func (a ItemAttribute) IsBetween(start, end any) Builder { return IsBetween(a, start, end) }

// TimestampEquals creates a condition checking if this timestamp attribute equals the given time,
// formatted as RFC3339.
func (a ItemAttribute) TimestampEquals(t time.Time) Builder { return TimestampEquals(a, t) }

// TimestampBetween creates a condition checking if this timestamp attribute falls between the start and end times,
// formatted at RFC3339.
func (a ItemAttribute) TimestampBetween(s, e time.Time) Builder { return TimestampBetween(a, s, e) }

// ExpiresAfter creates a condition checking if this timestamp attribute is after the given time.
func (a ItemAttribute) ExpiresAfter(t time.Time) Builder { return ExpiresAfter(a, t) }

// ExpiresIn creates a condition checking if this timestamp attribute is after the current time plus the given duration.
func (a ItemAttribute) ExpiresIn(d time.Duration) Builder { return ExpiresIn(a, d) }

// ExpiresInUTC creates a condition checking if this timestamp attribute is after the current UTC time plus the given duration.
func (a ItemAttribute) ExpiresInUTC(d time.Duration) Builder { return ExpiresInUTC(a, d) }

// Attribute creates an [ItemAttribute] from one or more string components that are joined with dots. Provide
// more than one key to query on nested structures in the item.
func Attribute(key ...string) ItemAttribute {
	path := strings.Join(key, ".")
	return ItemAttribute(path)
}

// Identity creates a new expression that always evaluates to true.
// This can be useful as a placeholder or default condition.
func Identity() Builder {
	return newBuilder(unaryCriteria{Operation: OperationTrue})
}

// Equals creates a new expression that compares an attribute for equality with a value.
func Equals[U any](a ItemAttribute, value U) Builder {
	return newBuilder(binaryCriteria{
		attribute: a.name(),
		Operation: OperationEqual,
		Value:     value,
	})
}

// NotEquals creates a condition checking if the given attribute does not equal the provided value.
func NotEquals[U any](a ItemAttribute, value U) Builder {
	return newBuilder(binaryCriteria{
		attribute: a.name(),
		Operation: OperationNotEqual,
		Value:     value,
	})
}

// LessThan creates a condition checking if the given attribute is less than the provided value.
func LessThan[U comparable](a ItemAttribute, value U) Builder {
	return newBuilder(binaryCriteria{
		attribute: a.name(),
		Operation: OperationLessThan,
		Value:     value,
	})
}

// LessThanEqual creates a condition checking if the given attribute is less than or equal
// to the provided value.
func LessThanEqual[U comparable](a ItemAttribute, value U) Builder {
	return newBuilder(binaryCriteria{
		attribute: a.name(),
		Operation: OperationLessThanEqual,
		Value:     value,
	})
}

// GreaterThan creates a condition checking if the given attribute is greater than the provided value.
func GreaterThan[U comparable](a ItemAttribute, value U) Builder {
	return newBuilder(binaryCriteria{
		attribute: a.name(),
		Operation: OperationGreaterThan,
		Value:     value,
	})
}

// GreaterThanEqual creates a condition checking if the given attribute is greater than or equal to
// the provided value.
func GreaterThanEqual[U comparable](a ItemAttribute, value U) Builder {
	return newBuilder(binaryCriteria{
		attribute: a.name(),
		Operation: OperationGreaterThanEqual,
		Value:     value,
	})
}

// Exists creates a condition checking if this attribute exists in the item.
func Exists(a ItemAttribute) Builder {
	return newBuilder(unaryCriteria{
		attribute: a.name(),
		Operation: OperationExists,
	})
}

// NotExists creates a condition checking if this attribute does not exist in the item.
func NotExists(a ItemAttribute) Builder {
	return newBuilder(unaryCriteria{
		attribute: a.name(),
		Operation: OperationNotExists,
	})
}

// HasSubstring creates a condition checking if this string attribute contains the given substring.
func HasSubstring(a ItemAttribute, substr string) Builder {
	return newBuilder(binaryCriteria{
		attribute: a.name(),
		Operation: OperationContains,
		Value:     substr,
	})
}

// Contains creates a condition checking if this set attribute contains any of the provided items.
// The attribute's value should be a list or set.
func Contains[U any](a ItemAttribute, items ...U) Builder {
	var curr Builder
	for idx, item := range items {
		expr := newBuilder(binaryCriteria{
			attribute: a.name(),
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

// Intersects creates a condition checking if this set attribute has any elements in common with the provided items.
// The attribute value should be a list or a set.
func Intersects[U any](a ItemAttribute, items ...U) Builder {
	var curr Builder
	for idx, item := range items {
		expr := newBuilder(binaryCriteria{
			attribute: a.name(),
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

// HasPrefix creates a condition checking if this string attribute begins with the given prefix.
func HasPrefix(a ItemAttribute, prefix string) Builder {
	return newBuilder(binaryCriteria{
		attribute: a.name(),
		Operation: OperationBeginsWith,
		Value:     prefix,
	})
}

// IsOneOf creates a condition checking if this attribute's value matches any of the provided values.
func IsOneOf[U any](a ItemAttribute, items ...U) Builder {
	anyitems := make([]any, 0, len(items))
	for _, item := range items {
		anyitems = append(anyitems, item)
	}
	return newBuilder(binaryCriteria{
		attribute: a.name(),
		Operation: OperationIn,
		Value:     anyitems,
	})
}

// IsBetween creates a condition checking if this attribute's value falls between the start and end values.
func IsBetween[U comparable](a ItemAttribute, left U, right U) Builder {
	return newBuilder(betweenCriteria{
		attribute: a.name(),
		Left:      left,
		Right:     right,
	})
}

// TimestampEquals creates a condition checking if this timestamp attribute equals the given time,
// formatted as RFC3339.
func TimestampEquals(a ItemAttribute, value time.Time) Builder {
	return Equals(a, value.Format(time.RFC3339))
}

// TimestampBetween creates a condition checking if this timestamp attribute falls between the start and end times,
// formatted at RFC3339.
func TimestampBetween(a ItemAttribute, start, end time.Time) Builder {
	return IsBetween(a, start.Format(time.RFC3339), end.Format(time.RFC3339))
}

// ExpiresAfter creates a condition checking if this timestamp attribute is after the given time.
func ExpiresAfter(a ItemAttribute, dt time.Time) Builder {
	return LessThanEqual(a, dt.UTC().Unix())
}

// ExpiresIn creates a condition checking if this timestamp attribute is after the current time plus the given duration.
func ExpiresIn(a ItemAttribute, d time.Duration) Builder {
	return ExpiresAfter(a, time.Now().Add(d))
}

// ExpiresInUTC creates a condition checking if this timestamp attribute is after the current UTC time plus the given duration.
func ExpiresInUTC(a ItemAttribute, d time.Duration) Builder {
	return ExpiresAfter(a, time.Now().UTC().Add(d))
}

// Expression defines the interface for building DynamoDB query expressions.
// It provides methods to convert expressions to string format and to generate
// DynamoDB condition builders.
type Expression interface {
	fmt.Stringer
	evaluate(inspector) error
	condition() expression.ConditionBuilder
	keyCondition() expression.KeyConditionBuilder
}

// Condition converts an Expression into a DynamoDB ConditionBuilder
// that can be used with the AWS SDK.
func Condition(e Expression) expression.ConditionBuilder {
	return e.condition()
}

// KeyCondition converts an Expression into a DynamoDB KeyConditionBuilder
// that can be used with the AWS SDK for key condition expressions.
func KeyCondition(e Expression) expression.KeyConditionBuilder {
	return e.keyCondition()
}

// binaryCriteria represents a comparison between an attribute and a value using a binary operation.
type binaryCriteria struct {
	attribute string    // The name of the attribute being compared
	Operation operation // The comparison operation to perform
	Value     any       // The value to compare against
}

func (b binaryCriteria) String() string {
	return fmt.Sprintf("('%s' %s '%v')", b.attribute, b.Operation, b.Value)
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
			expression.Name(b.attribute),
			b.Value.(string),
		)
	}
	if b.Operation == OperationContains {
		return expression.Contains(
			expression.Name(b.attribute),
			b.Value,
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
			expression.Name(b.attribute),
			right,
			others...,
		)
	}

	expr, ok := comparableConditions[b.Operation]
	if ok {
		return expr(expression.Name(b.attribute), expression.Value(b.Value))
	}

	panic(fmt.Errorf("unknown binary operation: '%s'", b.Operation))
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
			expression.Key(b.attribute),
			b.Value.(string),
		)
	}
	expr, ok := comparableKeyConditions[b.Operation]
	if ok {
		return expr(expression.Key(b.attribute), expression.Value(b.Value))
	}

	panic(fmt.Errorf("unknown binary operation: '%s'", b.Operation))
}

// unaryCriteria represents a unary operation on an attribute (like exists/not exists).
type unaryCriteria struct {
	attribute string    // The name of the attribute being checked
	Operation operation // The unary operation to perform
}

func (u unaryCriteria) String() string {
	if u.Operation == OperationTrue {
		return "(1=1)"
	}
	return fmt.Sprintf("%s('%s')", u.Operation, u.attribute)
}

func (u unaryCriteria) condition() (cb expression.ConditionBuilder) {
	if u.Operation == OperationExists {
		return expression.AttributeExists(expression.Name(u.attribute))
	}
	if u.Operation == OperationNotExists {
		return expression.AttributeNotExists(expression.Name(u.attribute))
	}
	if u.Operation == OperationTrue {
		return expression.Equal(expression.Value(1), expression.Value(1))
	}

	panic(fmt.Errorf("unknown unary operation: '%s'", u.Operation))
}

func (u unaryCriteria) keyCondition() (cb expression.KeyConditionBuilder) {
	panic(fmt.Errorf("cannot generate key condition from unary criteria %s", u))
}

// betweenCriteria represents a range comparison checking if an attribute's value falls between two bounds.
type betweenCriteria struct {
	attribute string // The name of the attribute being compared
	Left      any    // The lower bound of the range
	Right     any    // The upper bound of the range
}

func (b betweenCriteria) String() string {
	return fmt.Sprintf("BETWEEN('%s','%v','%v')", b.attribute, b.Left, b.Right)
}

func (b betweenCriteria) condition() (cb expression.ConditionBuilder) {
	return expression.Between(
		expression.Name(b.attribute),
		expression.Value(b.Left),
		expression.Value(b.Right),
	)
}

func (b betweenCriteria) keyCondition() (cb expression.KeyConditionBuilder) {
	return expression.KeyBetween(
		expression.Key(b.attribute),
		expression.Value(b.Left),
		expression.Value(b.Right),
	)
}

// orExpression represents a logical OR operation between two expressions.
type orExpression struct {
	left  Expression // The left-hand expression
	right Expression // The right-hand expression
}

func (o orExpression) String() string {
	return fmt.Sprintf("%s OR %s", o.left, o.right)
}

func (o orExpression) condition() (cb expression.ConditionBuilder) {
	return o.left.condition().Or(o.right.condition())
}

func (o orExpression) keyCondition() (cb expression.KeyConditionBuilder) { return }

// andExpression represents a logical AND between two expressions
// andExpression represents a logical AND operation between two expressions
type andExpression struct {
	left  Expression // The left-hand expression
	right Expression // The right-hand expression
}

func (a andExpression) String() string {
	return fmt.Sprintf("%s AND %s", a.left, a.right)
}

func (a andExpression) condition() (cb expression.ConditionBuilder) {
	return a.left.condition().And(a.right.condition())
}

func (a andExpression) keyCondition() (cb expression.KeyConditionBuilder) {
	return a.left.keyCondition().And(a.right.keyCondition())
}

// notExpression represents a logical NOT operation that negates the given expression.
type notExpression struct {
	expr Expression // The expression to negate
}

func (n notExpression) String() string {
	return fmt.Sprintf("NOT(%s)", n.expr)
}

func (n notExpression) condition() (cb expression.ConditionBuilder) {
	return n.expr.condition().Not()
}

func (n notExpression) keyCondition() (cb expression.KeyConditionBuilder) { return }

// Builder extends the [Expression] interface with methods for composing complex expressions
// through boolean operations and condition building capabilities.
type Builder interface {
	Expression
	// And creates a logical AND expression with another expression.
	And(Expression) Builder
	// Or creates a logical OR expression with another expression.
	Or(Expression) Builder
	// Not creates a logical NOT expression.
	Not() Builder
	// Condition returns a DynamoDB condition builder for use with AWS SDK.
	Condition() expression.ConditionBuilder
	// KeyCondition returns a DynamoDB key condition builder for use with AWS SDK.
	KeyCondition() expression.KeyConditionBuilder
}

// builder wraps an Expression and implements the Builder interface to provide
// fluent method chaining for building complex DynamoDB expressions.
type builder struct {
	expr Expression // The underlying expression being built
}

func (b builder) And(right Expression) Builder {
	return builder{expr: andExpression{left: b, right: right}}
}

// And creates a logical AND between two expressions
func And(left Expression, right Expression) Builder {
	return newBuilder(left).And(right)
}

func (b builder) Or(right Expression) Builder {
	return builder{expr: orExpression{left: b, right: right}}
}

// Or creates a logical OR between two expressions.
func Or(left Expression, right Expression) Builder {
	return newBuilder(left).Or(right)
}

func (b builder) Not() Builder {
	return builder{expr: notExpression{expr: b}}
}

func (b builder) Condition() expression.ConditionBuilder {
	return Condition(b)
}

func (b builder) KeyCondition() expression.KeyConditionBuilder {
	return KeyCondition(b)
}

// Not creates a new expression that represents the logical negation of the provided expression.
// It wraps the expression with DynamoDB's NOT operation.
func Not(expr Expression) Builder {
	return newBuilder(expr).Not()
}

// newBuilder creates a new builder wrapper around an [Expression].
func newBuilder(e Expression) builder { return builder{expr: e} }

// evaluate delegates the evaluation to the underlying expression.
func (b builder) evaluate(i inspector) error { return b.expr.evaluate(i) }

// condition returns the DynamoDB condition builder for this expression.
func (b builder) condition() expression.ConditionBuilder { return b.expr.condition() }

// keyCondition returns the DynamoDB key condition builder for this expression.
func (b builder) keyCondition() expression.KeyConditionBuilder { return b.expr.keyCondition() }

// String returns a string representation of the builder's expression.
func (b builder) String() string { return b.expr.String() }

// InspectToken represents different types of tokens encountered during expression tree traversal.
type InspectToken int

const (
	StartToken   InspectToken = iota // Marks the start of expression traversal
	EndToken                         // Marks the end of expression traversal
	BinaryToken                      // Represents a binary comparison operation
	UnaryToken                       // Represents a unary operation
	BetweenToken                     // Represents a between comparison
	AndToken                         // Represents a logical AND operation
	OrToken                          // Represents a logical OR operation
	NotToken                         // Represents a logical NOT operation
)

// Variant is a container for different types of expressions that can be inspected
// during expression traversal. Only one field will be non-nil at a time.
type Variant struct {
	Binary  *binaryCriteria  // A binary comparison operation
	Unary   *unaryCriteria   // A unary operation
	Between *betweenCriteria // A between comparison
	And     *andExpression   // A logical AND operation
	Or      *orExpression    // A logical OR operation
	Not     *notExpression   // A logical NOT operation
}

// inspector wraps a function that processes expression tokens during traversal
// inspector implements the logic for traversing and inspecting expression trees
type inspector struct {
	inspect func(InspectToken, Variant) error // Handler function for inspecting expressions
}

// Inspect traverses an expression tree and calls the provided function for each node.
// The function is called with tokens indicating the type of node and a Variant containing
// the node's data. This enables custom analysis of expression structures.
func Inspect(e Expression, fn func(InspectToken, Variant) error) error {
	if e == nil {
		return errors.New("cannot inspect nil expression")
	}
	if fn == nil {
		return errors.New("cannot inspect with nil function")
	}
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

// evaluateBinary handles inspection of binary comparison operations
func (i inspector) evaluateBinary(b *binaryCriteria) error {
	return i.inspect(BinaryToken, Variant{Binary: b})
}

// evaluateUnary processes a unary criteria expression node during traversal,
// calling the inspector's handler with the appropriate token and variant data
// evaluateUnary handles inspection of unary operations
func (i inspector) evaluateUnary(u *unaryCriteria) error {
	return i.inspect(UnaryToken, Variant{Unary: u})
}

// evaluateBetween processes a between criteria expression node during traversal,
// calling the inspector's handler with the appropriate token and variant data
// evaluateBetween handles inspection of between operations
func (i inspector) evaluateBetween(b *betweenCriteria) error {
	return i.inspect(BetweenToken, Variant{Between: b})
}

// evaluateOr handles inspection of OR operations
func (i inspector) evaluateOr(o *orExpression) error {
	return errors.Join(
		o.left.evaluate(i),
		i.inspect(OrToken, Variant{Or: o}),
		o.right.evaluate(i),
	)
}

// evaluateAnd handles inspection of AND operations
func (i inspector) evaluateAnd(a *andExpression) error {
	return errors.Join(
		a.left.evaluate(i),
		i.inspect(AndToken, Variant{And: a}),
		a.right.evaluate(i),
	)
}

// evaluateNot handles inspection of NOT operations
func (i inspector) evaluateNot(n *notExpression) error {
	return errors.Join(
		n.expr.evaluate(i),
		i.inspect(NotToken, Variant{Not: n}),
	)
}
