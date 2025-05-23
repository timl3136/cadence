// Code generated by MockGen. DO NOT EDIT.
// Source: interfaces.go
//
// Generated by this command:
//
//	mockgen -package pinot -source interfaces.go -destination generic_client_mock.go -self_package github.com/uber/cadence/common/pinot
//

// Package pinot is a generated GoMock package.
package pinot

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockGenericClient is a mock of GenericClient interface.
type MockGenericClient struct {
	ctrl     *gomock.Controller
	recorder *MockGenericClientMockRecorder
	isgomock struct{}
}

// MockGenericClientMockRecorder is the mock recorder for MockGenericClient.
type MockGenericClientMockRecorder struct {
	mock *MockGenericClient
}

// NewMockGenericClient creates a new mock instance.
func NewMockGenericClient(ctrl *gomock.Controller) *MockGenericClient {
	mock := &MockGenericClient{ctrl: ctrl}
	mock.recorder = &MockGenericClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockGenericClient) EXPECT() *MockGenericClientMockRecorder {
	return m.recorder
}

// CountByQuery mocks base method.
func (m *MockGenericClient) CountByQuery(query string) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountByQuery", query)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountByQuery indicates an expected call of CountByQuery.
func (mr *MockGenericClientMockRecorder) CountByQuery(query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountByQuery", reflect.TypeOf((*MockGenericClient)(nil).CountByQuery), query)
}

// GetTableName mocks base method.
func (m *MockGenericClient) GetTableName() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTableName")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetTableName indicates an expected call of GetTableName.
func (mr *MockGenericClientMockRecorder) GetTableName() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTableName", reflect.TypeOf((*MockGenericClient)(nil).GetTableName))
}

// Search mocks base method.
func (m *MockGenericClient) Search(request *SearchRequest) (*SearchResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Search", request)
	ret0, _ := ret[0].(*SearchResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Search indicates an expected call of Search.
func (mr *MockGenericClientMockRecorder) Search(request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Search", reflect.TypeOf((*MockGenericClient)(nil).Search), request)
}

// SearchAggr mocks base method.
func (m *MockGenericClient) SearchAggr(request *SearchRequest) (AggrResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SearchAggr", request)
	ret0, _ := ret[0].(AggrResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SearchAggr indicates an expected call of SearchAggr.
func (mr *MockGenericClientMockRecorder) SearchAggr(request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SearchAggr", reflect.TypeOf((*MockGenericClient)(nil).SearchAggr), request)
}
