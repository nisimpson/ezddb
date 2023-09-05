// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	context "context"

	dynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	mock "github.com/stretchr/testify/mock"
)

// MockScanner is an autogenerated mock type for the Scanner type
type MockScanner struct {
	mock.Mock
}

type MockScanner_Expecter struct {
	mock *mock.Mock
}

func (_m *MockScanner) EXPECT() *MockScanner_Expecter {
	return &MockScanner_Expecter{mock: &_m.Mock}
}

// Scan provides a mock function with given fields: _a0, _a1, _a2
func (_m *MockScanner) Scan(_a0 context.Context, _a1 *dynamodb.ScanInput, _a2 ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	_va := make([]interface{}, len(_a2))
	for _i := range _a2 {
		_va[_i] = _a2[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _a0, _a1)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 *dynamodb.ScanOutput
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)); ok {
		return rf(_a0, _a1, _a2...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) *dynamodb.ScanOutput); ok {
		r0 = rf(_a0, _a1, _a2...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*dynamodb.ScanOutput)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) error); ok {
		r1 = rf(_a0, _a1, _a2...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockScanner_Scan_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Scan'
type MockScanner_Scan_Call struct {
	*mock.Call
}

// Scan is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 *dynamodb.ScanInput
//   - _a2 ...func(*dynamodb.Options)
func (_e *MockScanner_Expecter) Scan(_a0 interface{}, _a1 interface{}, _a2 ...interface{}) *MockScanner_Scan_Call {
	return &MockScanner_Scan_Call{Call: _e.mock.On("Scan",
		append([]interface{}{_a0, _a1}, _a2...)...)}
}

func (_c *MockScanner_Scan_Call) Run(run func(_a0 context.Context, _a1 *dynamodb.ScanInput, _a2 ...func(*dynamodb.Options))) *MockScanner_Scan_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]func(*dynamodb.Options), len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(func(*dynamodb.Options))
			}
		}
		run(args[0].(context.Context), args[1].(*dynamodb.ScanInput), variadicArgs...)
	})
	return _c
}

func (_c *MockScanner_Scan_Call) Return(_a0 *dynamodb.ScanOutput, _a1 error) *MockScanner_Scan_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockScanner_Scan_Call) RunAndReturn(run func(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)) *MockScanner_Scan_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockScanner creates a new instance of MockScanner. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockScanner(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockScanner {
	mock := &MockScanner{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
