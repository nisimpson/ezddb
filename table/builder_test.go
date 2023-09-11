package table_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/table"
	"github.com/stretchr/testify/assert"
)

func failedMarshaler(item any) (ezddb.Item, error) { return nil, errors.New("mock error") }

func failedListMarshaler(items []ezddb.Item, out any) error { return errors.New("mock error") }

func failedUnmarshaler(item ezddb.Item, out any) error { return errors.New("mock error") }

func failedExpression(builder expression.Builder) (expression.Expression, error) {
	return expression.Expression{}, errors.New("mock error")
}

type mockPaginator struct {
	token    string
	startKey ezddb.Item
	fail     bool
}

func (s mockPaginator) GetStartKey(ctx context.Context, token string) (ezddb.Item, error) {
	if s.fail {
		return nil, errors.New("mock error")
	}
	return s.startKey, nil
}

func (s mockPaginator) GetStartKeyToken(context.Context, ezddb.Item) (string, error) {
	if s.fail {
		return "", errors.New("mock error")
	}
	return s.token, nil
}

func (s mockPaginator) fails() mockPaginator {
	s.fail = true
	return s
}

func TestNew(t *testing.T) {
	type args struct {
		tableName string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "creates new bulder",
			args: args{tableName: "my-test-table"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := table.New(tt.args.tableName)
			assert.NotNil(t, got)
			assert.EqualValues(t, tt.args.tableName, got.TableName())
		})
	}
}
