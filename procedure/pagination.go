package procedure

import (
	"context"

	"github.com/nisimpson/ezddb"
)

// StartKeyTokenProvider provides a method for converting a dynamodb
// evaluated start key into an opaque token.
type StartKeyTokenProvider interface {
	// GetStartKeyToken gets the opaque token from the provided start key.
	GetStartKeyToken(context.Context, ezddb.Item) (string, error)
}

// StartKeyProvider provides a method for converting an opaque
// token to a start key used in dynamodb pagination.
type StartKeyProvider interface {
	// GetStartKey gets the start key from the provided token.
	GetStartKey(ctx context.Context, token string) (ezddb.Item, error)
}
