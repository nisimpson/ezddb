package table

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/procedure"
)

type GetBuilder struct {
	builder Builder
	key     ItemKeyProvider
}

func (g GetBuilder) Build() procedure.Get {
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		return &dynamodb.GetItemInput{
			TableName: &g.builder.tableName,
			Key:       g.key.DynamoItemKey(),
		}, nil
	}
}

func (g GetBuilder) Execute(ctx context.Context, getter ezddb.Getter) GetResult {
	result, err := g.Build().Execute(ctx, getter)
	return GetResult{
		get:    g,
		output: result,
		error:  err,
	}
}

type GetResult struct {
	get    GetBuilder
	output *dynamodb.GetItemOutput
	error  error
}

func (g GetResult) UnmarshalItem(out any) error {
	if g.error != nil {
		return g.error
	} else if g.output == nil {
		panic(errors.New("no output to unmarshal"))
	}
	return g.get.builder.unmarshalMap(g.output.Item, out)
}

func (g GetResult) Error() error {
	return g.error
}
