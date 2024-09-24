package ezddb_test

import (
	"errors"
	"testing"

	"github.com/nisimpson/ezddb"
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
	OperationFails bool
	tableName      string
}

func (t table) failsTo() table {
	t.OperationFails = true
	return t
}

type fixture struct {
	t *testing.T
	ezddb.PutModifier
}
