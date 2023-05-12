package procedures

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type StartKey = map[string]types.AttributeValue

type StartKeyTokenProvider interface {
	GetStartKeyToken(context.Context, StartKey) (string, error)
}

type StartKeyProvider interface {
	GetStartKey(ctx context.Context, token string) (StartKey, error)
}
