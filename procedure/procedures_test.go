package procedure_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var ErrMock = errors.New("mock error")
var ErrAssertion = errors.New("assertion error")

type fixture struct {
	t           *testing.T
}
