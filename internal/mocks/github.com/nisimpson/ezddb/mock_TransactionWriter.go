// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	context "context"

	dynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	mock "github.com/stretchr/testify/mock"
)

// MockTransactionWriter is an autogenerated mock type for the TransactionWriter type
type MockTransactionWriter struct {
	mock.Mock
}

type MockTransactionWriter_Expecter struct {
	mock *mock.Mock
}

func (_m *MockTransactionWriter) EXPECT() *MockTransactionWriter_Expecter {
	return &MockTransactionWriter_Expecter{mock: &_m.Mock}
}

// TransactWriteItems provides a mock function with given fields: _a0, _a1, _a2
func (_m *MockTransactionWriter) TransactWriteItems(_a0 context.Context, _a1 *dynamodb.TransactWriteItemsInput, _a2 ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	_va := make([]interface{}, len(_a2))
	for _i := range _a2 {
		_va[_i] = _a2[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _a0, _a1)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 *dynamodb.TransactWriteItemsOutput
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)); ok {
		return rf(_a0, _a1, _a2...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) *dynamodb.TransactWriteItemsOutput); ok {
		r0 = rf(_a0, _a1, _a2...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*dynamodb.TransactWriteItemsOutput)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) error); ok {
		r1 = rf(_a0, _a1, _a2...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockTransactionWriter_TransactWriteItems_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'TransactWriteItems'
type MockTransactionWriter_TransactWriteItems_Call struct {
	*mock.Call
}

// TransactWriteItems is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 *dynamodb.TransactWriteItemsInput
//   - _a2 ...func(*dynamodb.Options)
func (_e *MockTransactionWriter_Expecter) TransactWriteItems(_a0 interface{}, _a1 interface{}, _a2 ...interface{}) *MockTransactionWriter_TransactWriteItems_Call {
	return &MockTransactionWriter_TransactWriteItems_Call{Call: _e.mock.On("TransactWriteItems",
		append([]interface{}{_a0, _a1}, _a2...)...)}
}

func (_c *MockTransactionWriter_TransactWriteItems_Call) Run(run func(_a0 context.Context, _a1 *dynamodb.TransactWriteItemsInput, _a2 ...func(*dynamodb.Options))) *MockTransactionWriter_TransactWriteItems_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]func(*dynamodb.Options), len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(func(*dynamodb.Options))
			}
		}
		run(args[0].(context.Context), args[1].(*dynamodb.TransactWriteItemsInput), variadicArgs...)
	})
	return _c
}

func (_c *MockTransactionWriter_TransactWriteItems_Call) Return(_a0 *dynamodb.TransactWriteItemsOutput, _a1 error) *MockTransactionWriter_TransactWriteItems_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockTransactionWriter_TransactWriteItems_Call) RunAndReturn(run func(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)) *MockTransactionWriter_TransactWriteItems_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockTransactionWriter creates a new instance of MockTransactionWriter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockTransactionWriter(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockTransactionWriter {
	mock := &MockTransactionWriter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
