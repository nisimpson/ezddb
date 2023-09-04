package procedure

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
)

// Query functions generate dynamodb input data given some context.
type Query func(context.Context) (*dynamodb.QueryInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (q Query) Invoke(ctx context.Context) (*dynamodb.QueryInput, error) {
	return q(ctx)
}


// QueryModifier makes modifications to the scan input before the procedure is executed.
type QueryModifier interface {
	// ModifyQueryInput is invoked when this modifier is applied to the provided input.
	ModifyQueryInput(context.Context, *dynamodb.QueryInput) error
}

// QueryModifierFunc is a function that implements QueryModifier.
type QueryModifierFunc modifier[dynamodb.QueryInput]

func (q QueryModifierFunc) ModifyQueryInput(ctx context.Context, input *dynamodb.QueryInput) error {
	return q(ctx, input)
}


// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (p Query) Modify(modifiers ...QueryModifier) Query {
	mapper := func(ctx context.Context, input *dynamodb.QueryInput, mod QueryModifier) error {
		return mod.ModifyQueryInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.QueryInput, error) {
		return modify[dynamodb.QueryInput](ctx, p, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (p Query) Execute(ctx context.Context,
	querier ezddb.Querier, options ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if input, err := p.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return querier.Query(ctx, input, options...)
	}
}

// WithPagination creates a new procedure that exhastively retrieves items from the
// database using the initial procedure. Use the callback to access data from each
// response.
func (p Query) WithPagination(callback PageQueryCallback) QueryExecutor {
	return func(ctx context.Context, q ezddb.Querier, options ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
		input, err := p.Invoke(ctx)
		if err != nil {
			return nil, err
		}
		for {
			if output, err := q.Query(ctx, input, options...); err != nil {
				return nil, err
			} else if ok := callback(ctx, output); !ok {
				return output, nil
			} else if output.LastEvaluatedKey == nil {
				return output, nil
			} else {
				input.ExclusiveStartKey = output.LastEvaluatedKey
			}
		}
	}
}

// PageQueryCallback is invoked each time the stored procedure is executed. The result
// of the execution is provided for further processing; to halt further page calls,
// return false.
type PageQueryCallback = func(context.Context, *dynamodb.QueryOutput) bool

// QueryExecutor functions execute the dynamoDB query items API.
type QueryExecutor func(context.Context, ezddb.Querier, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)

// Execute invokes the query items API using the provided querier and options.
func (q QueryExecutor) Execute(ctx context.Context, querier ezddb.Querier, options ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return q(ctx, querier, options...)
}
