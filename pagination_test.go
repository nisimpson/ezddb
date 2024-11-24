package ezddb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockStartKeyTokenProvider struct {
	mock.Mock
}

func (m *MockStartKeyTokenProvider) GetStartKeyToken(ctx context.Context, startKey StartKey) (string, error) {
	args := m.Called(ctx, startKey)
	return args.String(0), args.Error(1)
}

func (m *MockStartKeyTokenProvider) GetStartKey(ctx context.Context, token string) (StartKey, error) {
	args := m.Called(ctx, token)
	return args.Get(0).(StartKey), args.Error(1)
}

func TestGetStartKeyToken(t *testing.T) {
	provider := &MockStartKeyTokenProvider{}
	provider.On("GetStartKeyToken", mock.Anything, mock.Anything).Return("token", nil)

	startKey := StartKey{
		"key1": &types.AttributeValueMemberS{Value: "value1"},
		"key2": &types.AttributeValueMemberS{Value: "value2"},
	}

	token, err := GetStartKeyToken(context.Background(), provider, startKey)
	require.NoError(t, err)
	assert.Equal(t, "token", token)

	token, err = GetStartKeyToken(context.Background(), provider, nil)
	require.NoError(t, err)
	assert.Equal(t, "", token)

	provider.AssertExpectations(t)
}

func TestGetStartKey(t *testing.T) {
	provider := &MockStartKeyTokenProvider{}
	provider.On("GetStartKey", mock.Anything, "token").Return(StartKey{
		"key1": &types.AttributeValueMemberS{Value: "value1"},
		"key2": &types.AttributeValueMemberS{Value: "value2"},
	}, nil)

	startKey, err := GetStartKey(context.Background(), provider, "token")
	require.NoError(t, err)
	assert.Equal(t, StartKey{
		"key1": &types.AttributeValueMemberS{Value: "value1"},
		"key2": &types.AttributeValueMemberS{Value: "value2"},
	}, startKey)

	startKey, err = GetStartKey(context.Background(), provider, "")
	require.NoError(t, err)
	assert.Nil(t, startKey)

	provider.AssertExpectations(t)
}
