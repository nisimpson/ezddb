package table

import (
	"context"

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

func (g GetBuilder) Execute(ctx context.Context, getter ezddb.Getter, out any) error {
	result, err := g.Build().Execute(ctx, getter)
	if err != nil {
		return err
	}
	return g.builder.unmarshalMap(result.Item, out)
}
