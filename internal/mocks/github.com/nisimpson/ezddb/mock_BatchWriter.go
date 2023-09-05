// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	context "context"

	dynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	mock "github.com/stretchr/testify/mock"
)

// MockBatchWriter is an autogenerated mock type for the BatchWriter type
type MockBatchWriter struct {
	mock.Mock
}

type MockBatchWriter_Expecter struct {
	mock *mock.Mock
}

func (_m *MockBatchWriter) EXPECT() *MockBatchWriter_Expecter {
	return &MockBatchWriter_Expecter{mock: &_m.Mock}
}

// BatchWriteItem provides a mock function with given fields: _a0, _a1, _a2
func (_m *MockBatchWriter) BatchWriteItem(_a0 context.Context, _a1 *dynamodb.BatchWriteItemInput, _a2 ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	_va := make([]interface{}, len(_a2))
	for _i := range _a2 {
		_va[_i] = _a2[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _a0, _a1)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 *dynamodb.BatchWriteItemOutput
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)); ok {
		return rf(_a0, _a1, _a2...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) *dynamodb.BatchWriteItemOutput); ok {
		r0 = rf(_a0, _a1, _a2...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*dynamodb.BatchWriteItemOutput)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) error); ok {
		r1 = rf(_a0, _a1, _a2...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBatchWriter_BatchWriteItem_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'BatchWriteItem'
type MockBatchWriter_BatchWriteItem_Call struct {
	*mock.Call
}

// BatchWriteItem is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 *dynamodb.BatchWriteItemInput
//   - _a2 ...func(*dynamodb.Options)
func (_e *MockBatchWriter_Expecter) BatchWriteItem(_a0 interface{}, _a1 interface{}, _a2 ...interface{}) *MockBatchWriter_BatchWriteItem_Call {
	return &MockBatchWriter_BatchWriteItem_Call{Call: _e.mock.On("BatchWriteItem",
		append([]interface{}{_a0, _a1}, _a2...)...)}
}

func (_c *MockBatchWriter_BatchWriteItem_Call) Run(run func(_a0 context.Context, _a1 *dynamodb.BatchWriteItemInput, _a2 ...func(*dynamodb.Options))) *MockBatchWriter_BatchWriteItem_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]func(*dynamodb.Options), len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(func(*dynamodb.Options))
			}
		}
		run(args[0].(context.Context), args[1].(*dynamodb.BatchWriteItemInput), variadicArgs...)
	})
	return _c
}

func (_c *MockBatchWriter_BatchWriteItem_Call) Return(_a0 *dynamodb.BatchWriteItemOutput, _a1 error) *MockBatchWriter_BatchWriteItem_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBatchWriter_BatchWriteItem_Call) RunAndReturn(run func(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)) *MockBatchWriter_BatchWriteItem_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockBatchWriter creates a new instance of MockBatchWriter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockBatchWriter(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockBatchWriter {
	mock := &MockBatchWriter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
