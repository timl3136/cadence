// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/uber/cadence/common/activecluster (interfaces: ExternalEntityProvider)
//
// Generated by this command:
//
//	mockgen -package activecluster -destination external_entity_provider_mock.go -self_package github.com/uber/cadence/common/activecluster github.com/uber/cadence/common/activecluster ExternalEntityProvider
//

// Package activecluster is a generated GoMock package.
package activecluster

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockExternalEntityProvider is a mock of ExternalEntityProvider interface.
type MockExternalEntityProvider struct {
	ctrl     *gomock.Controller
	recorder *MockExternalEntityProviderMockRecorder
	isgomock struct{}
}

// MockExternalEntityProviderMockRecorder is the mock recorder for MockExternalEntityProvider.
type MockExternalEntityProviderMockRecorder struct {
	mock *MockExternalEntityProvider
}

// NewMockExternalEntityProvider creates a new mock instance.
func NewMockExternalEntityProvider(ctrl *gomock.Controller) *MockExternalEntityProvider {
	mock := &MockExternalEntityProvider{ctrl: ctrl}
	mock.recorder = &MockExternalEntityProviderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockExternalEntityProvider) EXPECT() *MockExternalEntityProviderMockRecorder {
	return m.recorder
}

// ChangeEvents mocks base method.
func (m *MockExternalEntityProvider) ChangeEvents() <-chan ChangeType {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ChangeEvents")
	ret0, _ := ret[0].(<-chan ChangeType)
	return ret0
}

// ChangeEvents indicates an expected call of ChangeEvents.
func (mr *MockExternalEntityProviderMockRecorder) ChangeEvents() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ChangeEvents", reflect.TypeOf((*MockExternalEntityProvider)(nil).ChangeEvents))
}

// GetExternalEntity mocks base method.
func (m *MockExternalEntityProvider) GetExternalEntity(ctx context.Context, entityKey string) (*ExternalEntity, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetExternalEntity", ctx, entityKey)
	ret0, _ := ret[0].(*ExternalEntity)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetExternalEntity indicates an expected call of GetExternalEntity.
func (mr *MockExternalEntityProviderMockRecorder) GetExternalEntity(ctx, entityKey any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetExternalEntity", reflect.TypeOf((*MockExternalEntityProvider)(nil).GetExternalEntity), ctx, entityKey)
}

// SupportedType mocks base method.
func (m *MockExternalEntityProvider) SupportedType() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SupportedType")
	ret0, _ := ret[0].(string)
	return ret0
}

// SupportedType indicates an expected call of SupportedType.
func (mr *MockExternalEntityProviderMockRecorder) SupportedType() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SupportedType", reflect.TypeOf((*MockExternalEntityProvider)(nil).SupportedType))
}
