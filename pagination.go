package ezddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// StartKey is the last dynamodb item that was retrieved in a query.
// Used by DynamoDB to retrieve the next page if provided as the
// start key in a Query or Scan request.
type StartKey = map[string]types.AttributeValue

// StartKeyTokenProvider provides a method for converting a dynamodb
// evaluated start key into an opaque token.
type StartKeyTokenProvider interface {
	// GetStartKeyToken gets the opaque token from the provided [StartKey].
	GetStartKeyToken(context.Context, StartKey) (string, error)
}

// GetStartKeyToken gets the opaque token from the provided [StartKey].
// If the start key is nil, an empty string is returned.
func GetStartKeyToken(ctx context.Context, provider StartKeyTokenProvider, startKey StartKey) (string, error) {
	if startKey == nil {
		return "", nil
	}
	return provider.GetStartKeyToken(ctx, startKey)
}

// StartKeyProvider provides a method for converting an opaque
// token to a [StartKey] used for dynamodb pagination.
type StartKeyProvider interface {
	// GetStartKey gets the start key from the provided token.
	GetStartKey(ctx context.Context, token string) (StartKey, error)
}

// GetStartKey gets the last evaluated key from the provided token.
// If the token is empty, nil is returned.
func GetStartKey(ctx context.Context, provider StartKeyProvider, token string) (StartKey, error) {
	if token == "" {
		return nil, nil
	}
	return provider.GetStartKey(ctx, token)
}
