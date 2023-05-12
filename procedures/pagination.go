package procedures

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type StartKey = map[string]types.AttributeValue

// StartKeyTokenProvider provides a method for converting a dynamodb
// evaluated start key into an opaque token.
type StartKeyTokenProvider interface {
	// GetStartKeyToken gets the opaque token from the provided start key.
	GetStartKeyToken(context.Context, StartKey) (string, error)
}

// StartKeyProvider provides a method for converting an opaque
// token to a start key used in dynamodb pagination.
type StartKeyProvider interface {
	// GetStartKey gets the start key from the provided token.
	GetStartKey(ctx context.Context, token string) (StartKey, error)
}
