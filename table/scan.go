package table

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/procedure"
)

type ScanBuilder struct {
	builder         Builder
	filterCondition expression.ConditionBuilder
	startKey        procedure.ScanModifier
	limit           procedure.ScanModifier
	indexName       string
}

func (s ScanBuilder) WithFilterCondition(cond expression.ConditionBuilder) ScanBuilder {
	s.filterCondition = cond
	return s
}

func (s ScanBuilder) Index(name string) ScanBuilder {
	s.indexName = name
	return s
}

func (s ScanBuilder) StartFromKey(key ezddb.Item) ScanBuilder {
	s.startKey = procedure.StartKey(key)
	return s
}

func (s ScanBuilder) StartFromToken(token string) ScanBuilder {
	s.startKey = procedure.StartToken(token, s.builder.startKeyProvider)
	return s
}

func (s ScanBuilder) Limit(count int) ScanBuilder {
	s.limit = procedure.Limit(count)
	return s
}

func (q ScanBuilder) Build() procedure.Scan {
	proc := procedure.Scan(func(ctx context.Context) (*dynamodb.ScanInput, error) {
		var expr expression.Expression
		var err error

		builder := expression.NewBuilder()

		if q.filterCondition.IsSet() {
			builder.WithFilter(q.filterCondition)
		}

		expr, err = builder.Build()
		if err != nil {
			return nil, err
		}

		input := &dynamodb.ScanInput{
			TableName:                 &q.builder.tableName,
			FilterExpression:          expr.Filter(),
			ProjectionExpression:      expr.Projection(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
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

func (s ScanBuilder) Execute(ctx context.Context, scanner ezddb.Scanner) ScanResult {
	output, err := s.Build().Execute(ctx, scanner)
	return ScanResult{
		scan:    s,
		scanner: scanner,
		output:  output,
		error:   err,
	}
}

func (s ScanBuilder) Where(attribute string, expr FilterExpression) ScanBuilder {
	var cond expression.ConditionBuilder
	if s.filterCondition.IsSet() {
		cond = s.filterCondition.And(expr.filter(attribute))
	} else {
		cond = expr.filter(attribute)
	}
	s.filterCondition = cond
	return s
}

func (s ScanBuilder) And(attribute string, expr FilterExpression) ScanBuilder {
	return s.Where(attribute, expr)
}

func (s ScanBuilder) Or(attribute string, expr FilterExpression) ScanBuilder {
	var cond expression.ConditionBuilder
	if s.filterCondition.IsSet() {
		cond = s.filterCondition.Or(expr.filter(attribute))
	} else {
		cond = expr.filter(attribute)
	}
	s.filterCondition = cond
	return s
}

func (s ScanBuilder) AndNot(attribute string, expr FilterExpression) ScanBuilder {
	var cond expression.ConditionBuilder
	if s.filterCondition.IsSet() {
		cond = s.filterCondition.And(expr.filter(attribute).Not())
	} else {
		cond = expr.filter(attribute).Not()
	}
	s.filterCondition = cond
	return s
}

func (s ScanBuilder) OrNot(attribute string, expr FilterExpression) ScanBuilder {
	var cond expression.ConditionBuilder
	if s.filterCondition.IsSet() {
		cond = s.filterCondition.Or(expr.filter(attribute).Not())
	} else {
		cond = expr.filter(attribute).Not()
	}
	s.filterCondition = cond
	return s
}


type ScanResult struct {
	scan    ScanBuilder
	scanner ezddb.Scanner
	output  *dynamodb.ScanOutput
	error   error
}

func (s ScanResult) Error() error {
	return s.error
}

func (s ScanResult) Output() *dynamodb.ScanOutput {
	return s.output
}

func (s ScanResult) HasNext() bool {
	if s.error != nil {
		return false
	} else if s.output == nil {
		return false
	} else if s.output.LastEvaluatedKey == nil {
		return false
	}
	return true
}

func (s ScanResult) UnmarshalItems(out any) error {
	if s.error != nil {
		return s.error
	}
	return s.scan.builder.unmarshalList(s.output.Items, out)
}

func (s ScanResult) LastStartKey() ezddb.Item {
	if s.output == nil {
		return nil
	}
	return s.output.LastEvaluatedKey
}

func (s ScanResult) LastStartToken(ctx context.Context) (string, error) {
	if s.error != nil {
		return "", s.error
	} else if s.output == nil {
		return "", nil
	}
	return s.scan.
		builder.
		startTokenProvider.
		GetStartKeyToken(ctx, s.LastStartKey())
}

func (s ScanResult) NextPage(ctx context.Context) ScanResult {
	if !s.HasNext() {
		return s
	}
	proc := s.scan.Build().Modify(procedure.StartKey(s.output.LastEvaluatedKey))
	out, err := proc.Execute(ctx, s.scanner)
	s.output = out
	s.error = err
	return s
}
