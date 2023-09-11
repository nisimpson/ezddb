package table

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/procedure"
)

type QueryBuilder struct {
	builder           Builder
	hashKeyCondition  expression.KeyConditionBuilder
	rangeKeyCondition expression.KeyConditionBuilder
	filterCondition   expression.ConditionBuilder
	startKey          procedure.QueryModifier
	limit             procedure.QueryModifier
	indexName         string
	scanForward       bool
}

func (b Builder) Query(attribute string, expr HashExpression) QueryBuilder {
	return QueryBuilder{
		builder:          b,
		hashKeyCondition: expr.key(attribute),
	}
}

func (q QueryBuilder) WithFilterCondition(cond expression.ConditionBuilder) QueryBuilder {
	q.filterCondition = cond
	return q
}

func (q QueryBuilder) Index(name string) QueryBuilder {
	q.indexName = name
	return q
}

func (q QueryBuilder) Forward() QueryBuilder {
	q.scanForward = true
	return q
}

func (q QueryBuilder) StartFromKey(key ezddb.Item) QueryBuilder {
	q.startKey = procedure.StartKey(key)
	return q
}

func (q QueryBuilder) StartFromToken(token string) QueryBuilder {
	q.startKey = procedure.StartToken(token, q.builder.startKeyProvider)
	return q
}

func (q QueryBuilder) Limit(count int) QueryBuilder {
	q.limit = procedure.Limit(count)
	return q
}

func (q QueryBuilder) Build() procedure.Query {
	proc := procedure.Query(func(ctx context.Context) (*dynamodb.QueryInput, error) {
		var expr expression.Expression
		var err error

		keyCond := q.hashKeyCondition
		if q.rangeKeyCondition.IsSet() {
			keyCond = keyCond.And(q.rangeKeyCondition)
		}

		builder := expression.NewBuilder().WithKeyCondition(keyCond)

		if q.filterCondition.IsSet() {
			builder.WithFilter(q.filterCondition)
		}

		expr, err = q.builder.buildExpression(builder)
		if err != nil {
			return nil, err
		}

		input := &dynamodb.QueryInput{
			TableName:                 &q.builder.tableName,
			KeyConditionExpression:    expr.KeyCondition(),
			FilterExpression:          expr.Filter(),
			ProjectionExpression:      expr.Projection(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ScanIndexForward:          &q.scanForward,
		}

		if q.indexName != "" {
			input.IndexName = &q.indexName
		}

		return input, nil
	})

	if q.startKey != nil {
		proc = proc.Modify(q.startKey)
	}
	if q.limit != nil {
		proc = proc.Modify(q.limit)
	}

	return proc
}

func (q QueryBuilder) Execute(ctx context.Context, querier ezddb.Querier) QueryResult {
	output, err := q.Build().Execute(ctx, querier)
	return QueryResult{
		query:   q,
		querier: querier,
		output:  output,
		error:   err,
	}
}

func (q QueryBuilder) Range(attribute string, expr RangeExpression) QueryBuilder {
	q.rangeKeyCondition = expr.key(attribute)
	return q
}

func (q QueryBuilder) Where(attribute string, expr FilterExpression) QueryBuilder {
	var cond expression.ConditionBuilder = expr.filter(attribute)
	q.filterCondition = cond
	return q
}

func (q QueryBuilder) And(attribute string, expr FilterExpression) QueryBuilder {
	cond := q.filterCondition.And(expr.filter(attribute))
	q.filterCondition = cond
	return q
}

func (q QueryBuilder) Or(attribute string, expr FilterExpression) QueryBuilder {
	cond := q.filterCondition.Or(expr.filter(attribute))
	q.filterCondition = cond
	return q
}

type QueryResult struct {
	query   QueryBuilder
	querier ezddb.Querier
	output  *dynamodb.QueryOutput
	error   error
}

func (q QueryResult) Error() error {
	return q.error
}

func (q QueryResult) Output() *dynamodb.QueryOutput {
	return q.output
}

func (q QueryResult) HasNext() bool {
	if q.error != nil {
		return false
	} else if q.output == nil {
		return false
	} else if q.output.LastEvaluatedKey == nil {
		return false
	}
	return true
}

func (q QueryResult) UnmarshalItems(out any) error {
	if q.error != nil {
		return q.error
	}
	return q.query.builder.unmarshalList(q.output.Items, out)
}

func (q QueryResult) LastStartKey() ezddb.Item {
	if q.output == nil {
		return nil
	}
	return q.output.LastEvaluatedKey
}

func (q QueryResult) LastStartToken(ctx context.Context) (string, error) {
	if q.error != nil {
		return "", q.error
	} else if q.output == nil {
		return "", nil
	}
	return q.query.
		builder.
		startTokenProvider.
		GetStartKeyToken(ctx, q.LastStartKey())
}

func (q QueryResult) NextPage(ctx context.Context) QueryResult {
	if !q.HasNext() {
		return q
	}
	proc := q.query.Build().Modify(procedure.StartKey(q.output.LastEvaluatedKey))
	out, err := proc.Execute(ctx, q.querier)
	q.output = out
	q.error = err
	return q
}
