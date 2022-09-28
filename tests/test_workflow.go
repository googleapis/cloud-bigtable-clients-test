// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Constants for CBT server and resources
const (
	mockServerAddr = "localhost:0"
	projectID      = "project"
	instanceID     = "instance"
)

// buildTableName returns a full table name using the given `tableID`.
func buildTableName(tableID string) string {
	return fmt.Sprintf("projects/%s/instances/%s/tables/%s", projectID, instanceID, tableID)
}

// createCbtClient creates a CBT client in the test proxy. The client is given an ID `clientID`, and
// it will target the given server `serverAddr` with custom timeout setting `timeout`. Creation
// error will cause the test to fail immediately (e.g., client ID collision).
func createCbtClient(t *testing.T, clientID string, serverAddr string, timeout *durationpb.Duration) {
	req := testproxypb.CreateClientRequest{
		ClientId:   clientID,
		DataTarget: serverAddr,
		ProjectId:  projectID,
		InstanceId: instanceID,
		ChannelCredential: &testproxypb.ChannelCredential{
			Value: &testproxypb.ChannelCredential_None{
				None: &empty.Empty{},
			},
		},
		PerOperationTimeout: timeout,
	}
	_, err := testProxyClient.CreateClient(context.Background(), &req)

	if err != nil {
		t.Fatalf("cbt client creation failed: %v", err)
	}
}

// removeCbtClient removes a CBT client in the test proxy by its ID `cientID`. Any failure here will
// cause the test to fail immediately (e.g., there is a bug in the proxy).
func removeCbtClient(t *testing.T, clientID string) {
	req := testproxypb.RemoveClientRequest{ClientId: clientID}
	_, err := testProxyClient.RemoveClient(context.Background(), &req)

	if err != nil {
		t.Fatalf("cbt client removal failed: %v", err)
	}
}

// doReadRowOps performs a ReadRow operation using the test proxy request `req` and the Bigtable
// server mock function `mockFn`. Non-nil `timeout` will override the default setting of Bigtable
// client. The test proxy results will be returned, where a nil element indicates proxy failure.
func doReadRowOps(
	t *testing.T,
	mockFn func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error,
	reqs []*testproxypb.ReadRowRequest,
	timeout *durationpb.Duration) []*testproxypb.RowResult {

	if len(reqs) == 0 {
		return nil
	}

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.ReadRowsFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy. The other requests should use the same client,
	// otherwise, there will be fatal failure.
	clientID := reqs[0].GetClientId()
	createCbtClient(t, clientID, server.Addr, timeout)
	defer removeCbtClient(t, clientID)

	// Ask the CBT client to do ReadRow via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.RowResult, len(reqs))
	for i := range reqs {
		if reqs[i].GetClientId() != clientID {
			t.Fatalf("Proxy requests in a test case should use the same client ID")
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.ReadRow(context.Background(), reqs[i])
			if err == nil {
				results[i] = res
			} else {
				t.Logf("The RPC to test proxy encountered error: %v", err)
				results[i] = nil
			}
		}(i)
	}
	wg.Wait()

	return results
}

// doReadRowOp performs a ReadRow operation using the test proxy request `req` and the mock server
// function `mockFn`. Non-nil `timeout` will override the default setting of Cloud Bigtable client.
// The test proxy result will be returned, where nil value indicates proxy failure.
func doReadRowOp(
	t *testing.T,
	mockFn func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error,
	req *testproxypb.ReadRowRequest,
	timeout *durationpb.Duration) *testproxypb.RowResult {

	results := doReadRowOps(t, mockFn, []*testproxypb.ReadRowRequest{req}, timeout)
	return results[0]
}

// doReadRowsOps performs ReadRows operations using the test proxy requests `reqs` and the mock
// server function `mockFn`. Non-nil `timeout` will override the default setting of Cloud Bigtable
// client. The test proxy results will be returned, where a nil element indicates proxy failure.
func doReadRowsOps(
	t *testing.T,
	mockFn func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error,
	reqs []*testproxypb.ReadRowsRequest,
	timeout *durationpb.Duration) []*testproxypb.RowsResult {

	if len(reqs) == 0 {
		return nil
	}

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.ReadRowsFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy. The other requests should use the same client,
	// otherwise, there will be fatal failure.
	clientID := reqs[0].GetClientId()
	createCbtClient(t, clientID, server.Addr, timeout)
	defer removeCbtClient(t, clientID)

	// Ask the CBT client to do ReadRows via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.RowsResult, len(reqs))
	for i := range reqs {
		if reqs[i].GetClientId() != clientID {
			t.Fatalf("Proxy requests in a test case should use the same client ID")
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.ReadRows(context.Background(), reqs[i])
			if err == nil {
				results[i] = res
			} else {
				t.Logf("The RPC to test proxy encountered error: %v", err)
				results[i] = nil
			}
		}(i)
	}
	wg.Wait()

	return results
}

// doReadRowsOp performs a ReadRows operation using the test proxy request `req` and the mock server
// function `mockFn`. Non-nil `timeout` will override the default setting of Cloud Bigtable client.
// The test proxy result will be returned, where nil value indicates proxy failure.
func doReadRowsOp(
	t *testing.T,
	mockFn func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error,
	req *testproxypb.ReadRowsRequest,
	timeout *durationpb.Duration) *testproxypb.RowsResult {

	results := doReadRowsOps(t, mockFn, []*testproxypb.ReadRowsRequest{req}, timeout)
	return results[0]
}

// doMutateRowOps performs MutateRow operations using the test proxy request `reqs` and the mock
// server function `mockFn`. Non-nil `timeout` will override the default setting of Cloud Bigtable
// client. The test proxy results will be returned, where a nil element indicates proxy failure.
func doMutateRowOps(
	t *testing.T,
	mockFn func(context.Context, *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error),
	reqs []*testproxypb.MutateRowRequest,
	timeout *durationpb.Duration) []*testproxypb.MutateRowResult {

	if len(reqs) == 0 {
		return nil
	}

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.MutateRowFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy. The other requests should use the same client,
	// otherwise, there will be fatal failure.
	clientID := reqs[0].GetClientId()
	createCbtClient(t, clientID, server.Addr, timeout)
	defer removeCbtClient(t, clientID)

	// Ask the CBT client to do MutateRow via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.MutateRowResult, len(reqs))
	for i := range reqs {
		if reqs[i].GetClientId() != clientID {
			t.Fatalf("Proxy requests in a test case should use the same client ID")
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.MutateRow(context.Background(), reqs[i])
			if err == nil {
				results[i] = res
			} else {
				t.Logf("The RPC to test proxy encountered error: %v", err)
				results[i] = nil
			}
		}(i)
	}
	wg.Wait()

	return results
}

// doMutateRowOp performs a MutateRow operation using the test proxy request `req` and the mock
// server function `mockFn`. Non-nil `timeout` will override the default setting of Cloud Bigtable
// client. The test proxy result will be returned, where nil value indicates proxy failure.
func doMutateRowOp(
	t *testing.T,
	mockFn func(context.Context, *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error),
	req *testproxypb.MutateRowRequest,
	timeout *durationpb.Duration) *testproxypb.MutateRowResult {

	results := doMutateRowOps(t, mockFn, []*testproxypb.MutateRowRequest{req}, timeout)
	return results[0]
}


// doMutateRowsOps performs MutateRows operations using the test proxy requests `reqs` and the mock
// server function `mockFn`. Non-nil `timeout` will override the default setting of Cloud Bigtable
// client. The test proxy results will be returned, where a nil element indicates proxy failure.
func doMutateRowsOps(
	t *testing.T,
	mockFn func(*btpb.MutateRowsRequest, btpb.Bigtable_MutateRowsServer) error,
	reqs []*testproxypb.MutateRowsRequest,
	timeout *durationpb.Duration) []*testproxypb.MutateRowsResult {

	if len(reqs) == 0 {
		return nil
	}

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.MutateRowsFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy. The other requests should use the same client,
	// otherwise, there will be fatal failure.
	clientID := reqs[0].GetClientId()
	createCbtClient(t, clientID, server.Addr, timeout)
	defer removeCbtClient(t, clientID)

	// Ask the CBT client to do MutateRows via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.MutateRowsResult, len(reqs))
	for i := range reqs {
		if reqs[i].GetClientId() != clientID {
			t.Fatalf("Proxy requests in a test case should use the same client ID")
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.BulkMutateRows(context.Background(), reqs[i])
			if err == nil {
				results[i] = res
			} else {
				t.Logf("The RPC to test proxy encountered error: %v", err)
				results[i] = nil
			}
		}(i)
	}
	wg.Wait()

	return results
}

// doMutateRowsOp performs a MutateRows operation using the test proxy request `req` and the mock
// server function `mockFn`. Non-nil `timeout` will override the default setting of Cloud Bigtable
// client. The test proxy result will be returned, where nil value indicates proxy failure.
func doMutateRowsOp(
	t *testing.T,
	mockFn func(*btpb.MutateRowsRequest, btpb.Bigtable_MutateRowsServer) error,
	req *testproxypb.MutateRowsRequest,
	timeout *durationpb.Duration) *testproxypb.MutateRowsResult {

	results := doMutateRowsOps(t, mockFn, []*testproxypb.MutateRowsRequest{req}, timeout)
	return results[0]
}

// doSampleRowKeysOps performs SampleRowKeys operations using the test proxy requests `reqs` and the
// mock server function `mockFn`. Non-nil `timeout` will override the default setting of Cloud
// Bigtable client. The test proxy results will be returned, where a nil element indicates
// proxy failure.
func doSampleRowKeysOps(
	t *testing.T,
	mockFn func(*btpb.SampleRowKeysRequest, btpb.Bigtable_SampleRowKeysServer) error,
	reqs []*testproxypb.SampleRowKeysRequest,
	timeout *durationpb.Duration) []*testproxypb.SampleRowKeysResult {

	if len(reqs) == 0 {
		return nil
	}

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.SampleRowKeysFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy. The other requests should use the same client,
	// otherwise, there will be fatal failure.
	clientID := reqs[0].GetClientId()
	createCbtClient(t, clientID, server.Addr, timeout)
	defer removeCbtClient(t, clientID)

	// Ask the CBT client to do SampleRowKeys via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.SampleRowKeysResult, len(reqs))
	for i := range reqs {
		if reqs[i].GetClientId() != clientID {
			t.Fatalf("Proxy requests in a test case should use the same client ID")
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.SampleRowKeys(context.Background(), reqs[i])
			if err == nil {
				results[i] = res
			} else {
				t.Logf("The RPC to test proxy encountered error: %v", err)
				results[i] = nil
			}
		}(i)
	}
	wg.Wait()

	return results
}

// doSampleRowKeysOp performs a SampleRowKeys operation using the test proxy request `req` and the
// mock server function `mockFn`. Non-nil `timeout` will override the default setting of Cloud
// Bigtable client. The test proxy result will be returned, where nil value indicates proxy failure.
func doSampleRowKeysOp(
	t *testing.T,
	mockFn func(*btpb.SampleRowKeysRequest, btpb.Bigtable_SampleRowKeysServer) error,
	req *testproxypb.SampleRowKeysRequest,
	timeout *durationpb.Duration) *testproxypb.SampleRowKeysResult {

	results := doSampleRowKeysOps(t, mockFn, []*testproxypb.SampleRowKeysRequest{req}, timeout)
	return results[0]
}

// doCheckAndMutateRowOps performs CheckAndMutateRow operations using the test proxy request `reqs`
// and the mock server function `mockFn`. Non-nil `timeout` will override the default setting of
// Cloud Bigtable client. The test proxy results will be returned, where a nil element indicates
// proxy failure.
func doCheckAndMutateRowOps(
	t *testing.T,
	mockFn func(context.Context, *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error),
	reqs []*testproxypb.CheckAndMutateRowRequest,
	timeout *durationpb.Duration) []*testproxypb.CheckAndMutateRowResult {

	if len(reqs) == 0 {
		return nil
	}

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.CheckAndMutateRowFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy. The other requests should use the same client,
	// otherwise, there will be fatal failure.
	clientID := reqs[0].GetClientId()
	createCbtClient(t, clientID, server.Addr, timeout)
	defer removeCbtClient(t, clientID)

	// Ask the CBT client to do CheckAndMutateRow via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.CheckAndMutateRowResult, len(reqs))
	for i := range reqs {
		if reqs[i].GetClientId() != clientID {
			t.Fatalf("Proxy requests in a test case should use the same client ID")
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.CheckAndMutateRow(context.Background(), reqs[i])
			if err == nil {
				results[i] = res
			} else {
				t.Logf("The RPC to test proxy encountered error: %v", err)
				results[i] = nil
			}
		}(i)
	}
	wg.Wait()

	return results
}

// doCheckAndMutateRowOp performs a CheckAndMutateRow operation using the test proxy request `req`
// and the mock server function `mockFn`. Non-nil `timeout` will override the default setting of
// Cloud Bigtable client. The test proxy result will be returned, where nil value indicates proxy
// failure.
func doCheckAndMutateRowOp(
	t *testing.T,
	mockFn func(context.Context, *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error),
	req *testproxypb.CheckAndMutateRowRequest,
	timeout *durationpb.Duration) *testproxypb.CheckAndMutateRowResult {

	results := doCheckAndMutateRowOps(t, mockFn, []*testproxypb.CheckAndMutateRowRequest{req}, timeout)
	return results[0]
}


// doReadModifyWriteRowOps performs ReadModifyWriteRow operations using the test proxy requests
// `reqs` and the mock server function `mockFn`. Non-nil `timeout` will override the default setting
// of Cloud Bigtable client. The test proxy results will be returned, where a nil element indicates
// proxy failure.
func doReadModifyWriteRowOps(
	t *testing.T,
	mockFn func(context.Context, *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error),
	reqs []*testproxypb.ReadModifyWriteRowRequest,
	timeout *durationpb.Duration) []*testproxypb.RowResult {

	if len(reqs) == 0 {
		return nil
	}

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.ReadModifyWriteRowFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy. The other requests should use the same client,
	// otherwise, there will be fatal failure.
	clientID := reqs[0].GetClientId()
	createCbtClient(t, clientID, server.Addr, timeout)
	defer removeCbtClient(t, clientID)

	// Ask the CBT client to do ReadModifyWriteRow via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.RowResult, len(reqs))
	for i := range reqs {
		if reqs[i].GetClientId() != clientID {
			t.Fatalf("Proxy requests in a test case should use the same client ID")
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.ReadModifyWriteRow(context.Background(), reqs[i])
			if err == nil {
				results[i] = res
			} else {
				t.Logf("The RPC to test proxy encountered error: %v", err)
				results[i] = nil
			}
		}(i)
	}
	wg.Wait()

	return results
}

// doReadModifyWriteRowOp performs a ReadModifyWriteRow operation using the test proxy request `req`
// and the mock server function `mockFn`. Non-nil `timeout` will override the default setting of
// Cloud Bigtable client. The test proxy result will be returned, where nil value indicates proxy
// failure.
func doReadModifyWriteRowOp(
	t *testing.T,
	mockFn func(context.Context, *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error),
	req *testproxypb.ReadModifyWriteRowRequest,
	timeout *durationpb.Duration) *testproxypb.RowResult {

	results := doReadModifyWriteRowOps(t, mockFn, []*testproxypb.ReadModifyWriteRowRequest{req}, timeout)
	return results[0]
}

// checkResultOkStatus checks if the results have ok status. The result type can be any of those
// supported by the test proxy.
func checkResultOkStatus[R anyResult](t *testing.T, results ...R) {
	for _, res := range results {
		assert.NotEmpty(t, res)
		assert.Empty(t, res.GetStatus().GetCode())
	}
}

// checkRequestsAreWithin checks if the requests are received within a certain period of time.
// The record type can be any of those supported by the mock server.
func checkRequestsAreWithin[R anyRecord](t *testing.T, periodMillisec int, records chan R) {
	minTsMs := int64(math.MaxInt64)
	maxTsMs := int64(math.MinInt64)
	close(records)
	for {
		loggedReq, more := <- records
		if !more {
			break
		}

		ts := loggedReq.GetTs().UnixMilli()
		if (minTsMs > ts) {
			minTsMs = ts
		}
		if (maxTsMs < ts) {
			maxTsMs = ts
		}
	}
	t.Logf("The requests were received within %dms", maxTsMs - minTsMs)
	assert.Less(t, maxTsMs - minTsMs, int64(periodMillisec))
}
