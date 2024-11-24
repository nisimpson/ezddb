package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAttribute(t *testing.T) {
	tests := []struct {
		name     string
		keys     []string
		expected string
	}{
		{
			name:     "single key",
			keys:     []string{"id"},
			expected: "id",
		},
		{
			name:     "nested keys",
			keys:     []string{"user", "address", "city"},
			expected: "user.address.city",
		},
		{
			name:     "empty key",
			keys:     []string{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attr := Attribute(tt.keys...)
			assert.Equal(t, tt.expected, attr.name())
		})
	}
}

func TestItemAttributeMethods(t *testing.T) {
	attr := Attribute("test")

	t.Run("KeyBuilder", func(t *testing.T) {
		kb := attr.KeyBuilder()
		assert.NotNil(t, kb)
	})

	t.Run("NameBuilder", func(t *testing.T) {
		nb := attr.NameBuilder()
		assert.NotNil(t, nb)
	})

	t.Run("Comparison operations", func(t *testing.T) {
		tests := []struct {
			name string
			expr Expression
		}{
			{"Equals", attr.Equals(5)},
			{"NotEquals", attr.NotEquals(5)},
			{"LessThan", attr.LessThan(5)},
			{"LessThanEqual", attr.LessThanEqual(5)},
			{"GreaterThan", attr.GreaterThan(5)},
			{"GreaterThanEqual", attr.GreaterThanEqual(5)},
			{"Exists", attr.Exists()},
			{"NotExists", attr.NotExists()},
			{"HasSubstring", attr.HasSubstring("test")},
			{"Contains", attr.Contains("item1", "item2")},
			{"Intersects", attr.Intersects("item1", "item2")},
			{"HasPrefix", attr.HasPrefix("test")},
			{"IsOneOf", attr.IsOneOf(1, 2, 3)},
			{"IsBetween", attr.IsBetween(1, 5)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.NotNil(t, tt.expr)
				assert.NotEmpty(t, tt.expr.String())
			})
		}
	})

	t.Run("Time operations", func(t *testing.T) {
		now := time.Now()
		tests := []struct {
			name string
			expr Expression
		}{
			{"TimestampEquals", attr.TimestampEquals(now)},
			{"TimestampBetween", attr.TimestampBetween(now, now.Add(time.Hour))},
			{"ExpiresAfter", attr.ExpiresAfter(now)},
			{"ExpiresIn", attr.ExpiresIn(time.Hour)},
			{"ExpiresInUTC", attr.ExpiresInUTC(time.Hour)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.NotNil(t, tt.expr)
				assert.NotEmpty(t, tt.expr.String())
			})
		}
	})
}

func TestLogicalOperations(t *testing.T) {
	attr1 := Attribute("field1")
	attr2 := Attribute("field2")

	expr1 := attr1.Equals(5)
	expr2 := attr2.Equals(10)

	t.Run("AND operation", func(t *testing.T) {
		and := expr1.And(expr2)
		assert.NotNil(t, and)
		assert.Contains(t, and.String(), "AND")
	})

	t.Run("OR operation", func(t *testing.T) {
		or := expr1.Or(expr2)
		assert.NotNil(t, or)
		assert.Contains(t, or.String(), "OR")
	})

	t.Run("NOT operation", func(t *testing.T) {
		not := expr1.Not()
		assert.NotNil(t, not)
		assert.Contains(t, not.String(), "NOT")
	})
}

func TestConditionBuilders(t *testing.T) {
	attr := Attribute("test")
	expr := attr.Equals(5)

	t.Run("Condition", func(t *testing.T) {
		cb := expr.Condition()
		assert.NotNil(t, cb)
	})

	t.Run("KeyCondition", func(t *testing.T) {
		kb := expr.KeyCondition()
		assert.NotNil(t, kb)
	})

	t.Run("Has Prefix", func(t *testing.T) {
		cb := attr.HasPrefix("prefix").Condition()
		assert.NotNil(t, cb)

		kb := attr.HasPrefix("prefix").KeyCondition()
		assert.NotNil(t, kb)
	})

	t.Run("Contains", func(t *testing.T) {
		cb := attr.Contains("item1", "item2").Condition()
		assert.NotNil(t, cb)
	})

	t.Run("IsOneOf", func(t *testing.T) {
		cb := attr.IsOneOf(1, 2, 3).Condition()
		assert.NotNil(t, cb)
	})

	t.Run("Unknown operation", func(t *testing.T) {
		cb := binaryCriteria{}
		uc := unaryCriteria{}
		assert.Panics(t, func() {
			cb.condition()
		})
		assert.Panics(t, func() {
			cb.keyCondition()
		})
		assert.Panics(t, func() {
			uc.keyCondition()
		})
		assert.Panics(t, func() {
			uc.condition()
		})
	})

	t.Run("Exists", func(t *testing.T) {
		cb := attr.Exists().Condition()
		assert.NotNil(t, cb)
	})

	t.Run("Not exists", func(t *testing.T) {
		cb := attr.NotExists().Condition()
		assert.NotNil(t, cb)
	})

	t.Run("Identity", func(t *testing.T) {
		cb := Identity().Condition()
		assert.NotNil(t, cb)
	})

	t.Run("Between", func(t *testing.T) {
		cb := attr.IsBetween(1, 5).Condition()
		assert.NotNil(t, cb)

		kb := attr.IsBetween(1, 5).KeyCondition()
		assert.NotNil(t, kb)
	})

	t.Run("Or", func(t *testing.T) {
		cb := Or(attr.Equals(1), attr.Equals(2)).Condition()
		assert.NotNil(t, cb)

		cb = attr.Equals(1).Or(attr.Equals(2)).Condition()
		assert.NotNil(t, cb)

		kb := Or(attr.Equals(1), attr.Equals(2)).KeyCondition()
		assert.NotNil(t, kb)
	})

	t.Run("And", func(t *testing.T) {
		cb := And(attr.Equals(1), attr.Equals(2)).Condition()
		assert.NotNil(t, cb)

		cb = attr.Equals(1).And(attr.Equals(2)).Condition()
		assert.NotNil(t, cb)

		kb := And(attr.Equals(1), attr.Equals(2)).KeyCondition()
		assert.NotNil(t, kb)
	})

	t.Run("Not", func(t *testing.T) {
		cb := attr.Equals(1).Not().Condition()
		assert.NotNil(t, cb)

		cb = Not(attr.Equals(1)).Condition()
		assert.NotNil(t, cb)

		kb := attr.Equals(1).Not().KeyCondition()
		assert.NotNil(t, kb)
	})
}

func TestIdentity(t *testing.T) {
	id := Identity()
	assert.NotNil(t, id)
	assert.Equal(t, "(1=1)", id.String())
}

func TestErrorCases(t *testing.T) {
	attr := Attribute("test")

	t.Run("Invalid key condition", func(t *testing.T) {
		expr := attr.NotExists()
		assert.Panics(t, func() {
			expr.KeyCondition()
		})
	})

	t.Run("Unary operation key condition", func(t *testing.T) {
		expr := attr.Exists()
		assert.Panics(t, func() {
			expr.KeyCondition()
		})
	})

	t.Run("Empty inspect handler", func(t *testing.T) {
		expr := attr.Equals(5)
		err := Inspect(expr, nil)
		assert.Error(t, err)
	})

	t.Run("Empty inspect expression", func(t *testing.T) {
		err := Inspect(nil, func(it InspectToken, v Variant) error { return nil })
		assert.Error(t, err)
	})
}

func TestComplexExpressions(t *testing.T) {
	attr1 := Attribute("field1")
	attr2 := Attribute("field2")
	attr3 := Attribute("field3")

	t.Run("Complex AND/OR combination", func(t *testing.T) {
		expr := attr1.Equals("value1").
			And(attr2.GreaterThan(100).Or(attr2.LessThan(50))).
			And(attr3.NotExists())

		assert.NotNil(t, expr)
		str := expr.String()
		assert.Contains(t, str, "'field1' = 'value1'")
		assert.Contains(t, str, "'field2' > '100'")
		assert.Contains(t, str, "'field2' < '50'")
		assert.Contains(t, str, "!exists('field3')")
		assert.Contains(t, str, "OR")
		assert.Contains(t, str, "AND")
	})

	t.Run("Multiple NOT operations", func(t *testing.T) {
		expr := attr1.Equals(10).Not().Not()
		assert.NotNil(t, expr)
		assert.Contains(t, expr.String(), "NOT")
	})

	t.Run("Between with timestamp", func(t *testing.T) {
		now := time.Now()
		later := now.Add(24 * time.Hour)
		expr := attr1.TimestampBetween(now, later)
		assert.NotNil(t, expr)
		assert.Contains(t, expr.String(), "BETWEEN")
	})
}

func TestEdgeCases(t *testing.T) {
	attr := Attribute("test")

	t.Run("Empty string value", func(t *testing.T) {
		expr := attr.Equals("")
		assert.NotNil(t, expr)
		assert.Contains(t, expr.String(), `= ''`)
	})

	t.Run("Nil value in Contains", func(t *testing.T) {
		expr := attr.Contains(nil)
		assert.NotNil(t, expr)
	})

	t.Run("Zero time value", func(t *testing.T) {
		expr := attr.TimestampEquals(time.Time{})
		assert.NotNil(t, expr)
	})

	t.Run("Zero duration", func(t *testing.T) {
		expr := attr.ExpiresIn(0)
		assert.NotNil(t, expr)
	})

	t.Run("Negative duration", func(t *testing.T) {
		expr := attr.ExpiresIn(-time.Hour)
		assert.NotNil(t, expr)
	})
}

func TestStringRepresentations(t *testing.T) {
	attr := Attribute("test")

	tests := []struct {
		name     string
		expr     Expression
		contains []string
	}{
		{
			name: "Equal with string",
			expr: attr.Equals("value"),
			contains: []string{
				"test",
				"=",
				"value",
			},
		},
		{
			name: "Between with numbers",
			expr: attr.IsBetween(1, 10),
			contains: []string{
				"test",
				"BETWEEN",
				"1",
				"10",
			},
		},
		{
			name: "Complex condition",
			expr: attr.GreaterThan(5).And(attr.LessThan(10)).Or(attr.Equals(15)),
			contains: []string{
				">",
				"<",
				"=",
				"AND",
				"OR",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := tt.expr.String()
			for _, substr := range tt.contains {
				assert.Contains(t, str, substr)
			}
		})
	}
}

func TestInspect(t *testing.T) {
	attr := Attribute("test")
	expr := attr.Exists().
		And(attr.Equals(5)).
		Or(attr.GreaterThan(2)).
		Or(Not(attr.IsBetween(0, 10)))

	var tokens []InspectToken
	err := Inspect(expr, func(token InspectToken, v Variant) error {
		tokens = append(tokens, token)
		return nil
	})

	assert.NoError(t, err)
	assert.Contains(t, tokens, StartToken)
	assert.Contains(t, tokens, EndToken)
	assert.Contains(t, tokens, BinaryToken)
	assert.Contains(t, tokens, BetweenToken)
	assert.Contains(t, tokens, UnaryToken)
	assert.Contains(t, tokens, AndToken)
	assert.Contains(t, tokens, OrToken)
	assert.Contains(t, tokens, NotToken)

	t.Log(tokens)
	t.Log(expr.String())
	//assert.FailNow(t, "Inspect tokens: %v", tokens)
}
