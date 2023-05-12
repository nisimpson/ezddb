package procedure_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

type putter struct {
	fixture
	dynamodb.PutItemOutput
	wantInput    *dynamodb.PutItemInput
	returnsError bool
}

func NewPutter(t *testing.T) putter {
	putter := putter{}
	putter.t = t
	return putter
}

func (p putter) PutItem(ctx context.Context, input *dynamodb.PutItemInput, options ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if p.returnsError {
		return nil, ErrMock
	} else if p.wantInput != nil && !assert.EqualValues(p.t, p.wantInput, input) {
		return nil, ErrAssertion
	} else {
		return &p.PutItemOutput, nil
	}
}

func (p putter) assertsInput(want *dynamodb.PutItemInput) putter {
	p.wantInput = want
	return p
}
