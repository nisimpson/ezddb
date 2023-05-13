package procedure_test

import (
	"errors"
	"testing"

	"github.com/nisimpson/ezddb/procedure"
)

var ErrMock = errors.New("mock error")
var ErrAssertion = errors.New("assertion error")

type customer struct {
	ID   string `dynamodbav:"id"`
	Name string `dynamodbav:"name"`
}

type order struct {
	ID         string `dynamodbav:"id"`
	CustomerID string `dynamodbav:"customer_id"`
}

func must[T any](item T, err error) T {
	if err != nil {
		panic(err)
	}
	return item
}

type table struct {
	procedureFails bool
	tableName      string
}

func (t table) failsTo() table {
	t.procedureFails = true
	return t
}

type fixture struct {
	t *testing.T
	procedure.PutModifier
}

func newFixture(t *testing.T) fixture {
	return fixture{t: t}
}

func (f fixture) withPutModifier(modifier procedure.PutModifier) fixture {
	f.PutModifier = modifier
	return f
}
