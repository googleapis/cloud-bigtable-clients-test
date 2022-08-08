// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"context"
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Constants for CBT server and resources. tableName is used by *_test.go files.
const (
	mockServerAddr = "localhost:0"
	projectID      = "project"
	instanceID     = "instance"
	tableName      = "projects/project/instances/instance/tables/table"
)

// createCbtClient creates a CBT client in the test proxy. The client is given an ID `clientID`, and
// it will target the given server `serverAddr` with custom timeout setting `timeout`. Any failure
// here will cause the test to fail immediately (e.g., there is client ID collision).
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

// runReadRowTest performs a ReadRow operation using the test proxy request `req` and the Bigtable
// server mock function `mockFn`. Non-nil `timeout` will override the default setting of Bigtable
// client. The test proxy's response will be returned.
func runReadRowTest(
	t *testing.T,
	mockFn func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error,
	req *testproxypb.ReadRowRequest, timeout *durationpb.Duration) *testproxypb.RowResult {

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.ReadRowsFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy
	createCbtClient(t, req.GetClientId(), server.Addr, timeout)
	defer removeCbtClient(t, req.GetClientId())

	// Ask the CBT client to do ReadRow via the test proxy
	res, err := testProxyClient.ReadRow(context.Background(), req)
	if err != nil {
		t.Fatalf("ReadRow request to test proxy failed: %v", err)
	}

	return res
}

// runReadRowsTest performs a ReadRows operation using the test proxy request `req` and the Bigtable
// server mock function `mockFn`. Non-nil `timeout` will override the default setting of Bigtable
// client. The test proxy's response will be returned.
func runReadRowsTest(
	t *testing.T,
	mockFn func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error,
	req *testproxypb.ReadRowsRequest, timeout *durationpb.Duration) *testproxypb.RowsResult {

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.ReadRowsFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy
	createCbtClient(t, req.GetClientId(), server.Addr, timeout)
	defer removeCbtClient(t, req.GetClientId())

	// Ask the CBT client to do ReadRows via the test proxy
	res, err := testProxyClient.ReadRows(context.Background(), req)
	if err != nil {
		t.Fatalf("ReadRows request to test proxy failed: %v", err)
	}

	return res
}

// runMutateRowTest performs a MutateRow operation using the test proxy request `req` and the
// Bigtable server mock function `mockFn`. Non-nil `timeout` will override the default setting of
// Bigtable client. The test proxy's response will be returned.
func runMutateRowTest(
	t *testing.T,
	mockFn func(context.Context, *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error),
	req *testproxypb.MutateRowRequest, timeout *durationpb.Duration) *testproxypb.MutateRowResult {

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.MutateRowFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy
	createCbtClient(t, req.GetClientId(), server.Addr, timeout)
	defer removeCbtClient(t, req.GetClientId())

	// Ask the CBT client to do MutateRow via the test proxy
	res, err := testProxyClient.MutateRow(context.Background(), req)
	if err != nil {
		t.Fatalf("MutateRow request to test proxy failed: %v", err)
	}

	return res
}

// runMutateRowsTest performs a MutateRows operation using the test proxy request `req` and the
// Bigtable server mock function `mockFn`. Non-nil `timeout` will override the default setting of
// Bigtable client. The test proxy's response will be returned.
func runMutateRowsTest(
	t *testing.T,
	mockFn func(*btpb.MutateRowsRequest, btpb.Bigtable_MutateRowsServer) error,
	req *testproxypb.MutateRowsRequest, timeout *durationpb.Duration) *testproxypb.MutateRowsResult {

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.MutateRowsFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy
	createCbtClient(t, req.GetClientId(), server.Addr, timeout)
	defer removeCbtClient(t, req.GetClientId())

	// Ask the CBT client to do MutateRows via the test proxy
	res, err := testProxyClient.BulkMutateRows(context.Background(), req)
	if err != nil {
		t.Fatalf("BulkMutateRows request to test proxy failed: %v", err)
	}

	return res
}

// runSampleRowKeysTest performs a SampleRowKeys operation using the test proxy request `req` and the
// Bigtable server mock function `mockFn`. Non-nil `timeout` will override the default setting of
// Bigtable client. The test proxy's response will be returned.
func runSampleRowKeysTest(
	t *testing.T,
	mockFn func(*btpb.SampleRowKeysRequest, btpb.Bigtable_SampleRowKeysServer) error,
	req *testproxypb.SampleRowKeysRequest, timeout *durationpb.Duration) *testproxypb.SampleRowKeysResult {

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.SampleRowKeysFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy
	createCbtClient(t, req.GetClientId(), server.Addr, timeout)
	defer removeCbtClient(t, req.GetClientId())

	// Ask the CBT client to do ReadRows via the test proxy
	res, err := testProxyClient.SampleRowKeys(context.Background(), req)
	if err != nil {
		t.Fatalf("SampleRowKeys request to test proxy failed: %v", err)
	}

	return res
}

// runCheckAndMutateRowTest performs a CheckAndMutateRow operation using the test proxy request `req`
// and the Bigtable server mock function `mockFn`. Non-nil `timeout` will override the default
// setting of Bigtable client. The test proxy's response will be returned.
func runCheckAndMutateRowTest(
	t *testing.T,
	mockFn func(context.Context, *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error),
	req *testproxypb.CheckAndMutateRowRequest, timeout *durationpb.Duration) *testproxypb.CheckAndMutateRowResult {

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.CheckAndMutateRowFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy
	createCbtClient(t, req.GetClientId(), server.Addr, timeout)
	defer removeCbtClient(t, req.GetClientId())

	// Ask the CBT client to do CheckAndMutateRow via the test proxy
	res, err := testProxyClient.CheckAndMutateRow(context.Background(), req)
	if err != nil {
		t.Fatalf("CheckAndMutateRow request to test proxy failed: %v", err)
	}

	return res
}

// runReadModifyWriteRowTest performs a ReadModifyWriteRow operation using the test proxy request
// `req` and the Bigtable server mock function `mockFn`. Non-nil `timeout` will override the
// default setting of Bigtable client. The test proxy's response will be returned.
func runReadModifyWriteRowTest(
	t *testing.T,
	mockFn func(context.Context, *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error),
	req *testproxypb.ReadModifyWriteRowRequest, timeout *durationpb.Duration) *testproxypb.RowResult {

	// Initialize a mock server with mockFn
	server, err := NewServer(mockServerAddr)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	server.ReadModifyWriteRowFn = mockFn

	// Start the mock server
	server.Start()
	defer server.Close()

	// Create a CBT client in the test proxy
	createCbtClient(t, req.GetClientId(), server.Addr, timeout)
	defer removeCbtClient(t, req.GetClientId())

	// Ask the CBT client to do CheckAndMutateRow via the test proxy
	res, err := testProxyClient.ReadModifyWriteRow(context.Background(), req)
	if err != nil {
		t.Fatalf("ReadModifyWriteRow request to test proxy failed: %v", err)
	}

	return res
}
