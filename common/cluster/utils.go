// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cluster

import (
	"errors"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

// GetOrUseDefaultActiveCluster return the current cluster name or use the input if valid
func GetOrUseDefaultActiveCluster(currentClusterName string, activeClusterName string) string {
	if len(activeClusterName) == 0 {
		return currentClusterName
	}
	return activeClusterName
}

// GetOrUseDefaultClusters return the current cluster or use the input if valid
func GetOrUseDefaultClusters(currentClusterName string, clusters []*persistence.ClusterReplicationConfig) []*persistence.ClusterReplicationConfig {
	if len(clusters) == 0 {
		return []*persistence.ClusterReplicationConfig{
			&persistence.ClusterReplicationConfig{
				ClusterName: currentClusterName,
			},
		}
	}
	return clusters
}

// WrapPeerHostname wraps an error with peer hostname information
func WrapPeerHostname(err error, peer string) error {
	if err == nil {
		return nil
	}
	if peer == "" {
		return err
	}
	return &types.PeerHostnameError{
		PeerHostname: peer,
		WrappedError: err,
	}
}

// ExtractPeerHostname extracts the peer hostname from a wrapped error
// Returns the hostname and the original unwrapped error
func ExtractPeerHostname(err error) (string, error) {
	if err == nil {
		return "", nil
	}
	var peerErr *types.PeerHostnameError
	current := err
	for current != nil {
		if errors.As(current, &peerErr) {
			return peerErr.PeerHostname, peerErr.WrappedError
		}
		current = errors.Unwrap(current)
	}
	return "", err
}
