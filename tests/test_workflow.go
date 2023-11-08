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
	"encoding/base64"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
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

// createCbtClient creates a CBT client with ID `clientID` in the test proxy. The client
// will target server at `serverAddr`. `opts` can be passed in to specify custom timeout
// and (or) app profile. Creation error will cause the test to fail immediately (e.g.,
// client ID collision).
func createCbtClient(t *testing.T, clientID string, serverAddr string, opts *clientOpts) {
	req := testproxypb.CreateClientRequest{
		ClientId:   clientID,
		DataTarget: serverAddr,
		ProjectId:  projectID,
		InstanceId: instanceID,
	}
	if opts != nil {
		req.AppProfileId = opts.profile
		req.PerOperationTimeout = opts.timeout
	}
	if *enableFeaturesAll {
		req.OptionalFeatureConfig = testproxypb.OptionalFeatureConfig_OPTIONAL_FEATURE_CONFIG_ENABLE_ALL
	}

	_, err := testProxyClient.CreateClient(context.Background(), &req)
	if err != nil {
		t.Fatalf("cbt client creation failed: %v", err)
	}
}

// closeCbtClient closes a CBT client in the test proxy by ID `cientID`. Any failure here will
// cause the test to fail immediately (e.g., there is a bug in the proxy).
func closeCbtClient(t *testing.T, clientID string) {
	req := testproxypb.CloseClientRequest{ClientId: clientID}
	_, err := testProxyClient.CloseClient(context.Background(), &req)

	// TODO: should ignore the possible error due to re-closing the client, as some test may
	// invoke client closing twice.
	if err != nil {
		t.Fatalf("cbt client closing failed: %v", err)
	}
}

// removeCbtClient removes a CBT client from the test proxy by ID `cientID` without closing it.
// Any failure here will cause the test to fail immediately (e.g., there is a bug in the proxy).
func removeCbtClient(t *testing.T, clientID string) {
	req := testproxypb.RemoveClientRequest{ClientId: clientID}
	_, err := testProxyClient.RemoveClient(context.Background(), &req)

	if err != nil {
		t.Fatalf("cbt client removal failed: %v", err)
	}
}

// initMockServer initializes a mock server without starting it or setting its behaviors.
// The optional argument `serverOpt` allows you to tune the server parameters.
func initMockServer(t *testing.T, serverOpt ...grpc.ServerOption) *Server {
	s, err := NewServer(mockServerAddr, serverOpt...)
	if err != nil {
		t.Fatalf("Server initialization failed: %v", err)
	}
	return s
}

// setUp starts the mock server and creates its accompanying client object.
func setUp(t *testing.T, s *Server, clientID string, opts *clientOpts) {
	s.Start()
	createCbtClient(t, clientID, s.Addr, opts)
}

// tearDown stops the mock server and removes its accompanying client object.
func tearDown(t *testing.T, s *Server, clientID string) {
	closeCbtClient(t, clientID)
	removeCbtClient(t, clientID)
	s.Close()
}

// validateClientID validates the client IDs in the proxy requests, which should be the same as the
// specified ID.
func validateClientID[R anyRequest](t *testing.T, reqs []R, clientID string) {
	for _, req := range reqs {
		if req.GetClientId() != clientID {
			t.Fatal("Requests in the same test case should use the same client ID")
		}
	}
}

// fillResults sets the `i`-th element of `results` with valid result or nil in the case of error.
func fillResults[R anyResult](t *testing.T, results []R, res R, err error, i int) {
	if err == nil {
		results[i] = res
	} else {
		t.Logf("The RPC to test proxy encountered error: %v", err)
		results[i] = nil
	}
}

// doReadRowOp is a simple wrapper of doReadRowOps. It's useful when there is only one ReadRow
// operation to perform. A single result will be returned, where nil value indicates proxy
// failure (not client's).
func doReadRowOp(
	t *testing.T,
	s *Server,
	req *testproxypb.ReadRowRequest,
	opts *clientOpts) *testproxypb.RowResult {

	results := doReadRowOps(t, s, []*testproxypb.ReadRowRequest{req}, opts)
	return results[0]
}

// doReadRowOps performs ReadRow operations in parallel, using the test proxy requests `reqs` and
// the mock server `s`. Non-nil `opts` will override the default client settings including app
// profile id and timeout. The results will be returned, where the i-th result corresponds to the
// i-th request. nil element indicates proxy failure (not client's).
// Note that the function manages the setup and teardown of resources.
func doReadRowOps(
	t *testing.T,
	s *Server,
	reqs []*testproxypb.ReadRowRequest,
	opts *clientOpts) []*testproxypb.RowResult {

	clientID := reqs[0].GetClientId()
	setUp(t, s, clientID, opts)
	defer tearDown(t, s, clientID)

	return doReadRowOpsCore(t, clientID, reqs, nil)
}

// doReadRowOpsCore does the work of sending concurrent requests to test proxy and collecting the
// results, where the i-th result corresponds to the i-th request. nil element indicates proxy
// failure (not client's). Non-nil `closeCbtClientAfter` will trigger Cloud Bigtable client being
// closed after sending off all the requests (>=1s delay should ensure the requests are already
// sent off when the client is closed).
// Note that the function doesn't manage the setup and teardown of resources.
func doReadRowOpsCore(
	t *testing.T,
	clientID string,
	reqs []*testproxypb.ReadRowRequest,
	closeCbtClientAfter *time.Duration) []*testproxypb.RowResult {

	validateClientID(t, reqs, clientID)

	// Ask the CBT client to do ReadRow via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.RowResult, len(reqs))
	for i := range reqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.ReadRow(context.Background(), reqs[i])
			fillResults(t, results, res, err, i)
		}(i)
	}
	if closeCbtClientAfter != nil {
		time.Sleep(*closeCbtClientAfter)
		closeCbtClient(t, clientID)
	}
	wg.Wait()

	return results
}

// doReadRowsOp is a simple wrapper of doReadRowsOps. It's useful when there is only one ReadRows
// operation to perform. A single result will be returned, where nil value indicates proxy
// failure (not client's).
func doReadRowsOp(
	t *testing.T,
	s *Server,
	req *testproxypb.ReadRowsRequest,
	opts *clientOpts) *testproxypb.RowsResult {

	results := doReadRowsOps(t, s, []*testproxypb.ReadRowsRequest{req}, opts)
	return results[0]
}

// doReadRowsOps performs ReadRows operations in parallel, using the test proxy requests `reqs` and
// the mock server `s`. Non-nil `opts` will override the default client settings including app
// profile id and timeout. The results will be returned, where the i-th result corresponds to the
// i-th request. nil element indicates proxy failure (not client's).
// Note that the function manages the setup and teardown of resources.
func doReadRowsOps(
	t *testing.T,
	s *Server,
	reqs []*testproxypb.ReadRowsRequest,
	opts *clientOpts) []*testproxypb.RowsResult {

	clientID := reqs[0].GetClientId()
	setUp(t, s, clientID, opts)
	defer tearDown(t, s, clientID)

	return doReadRowsOpsCore(t, clientID, reqs, nil)
}

// doReadRowsOpsCore does the work of sending concurrent requests to test proxy and collecting the
// results, where the i-th result corresponds to the i-th request. nil element indicates proxy
// failure (not client's). Non-nil `closeCbtClientAfter` will trigger Cloud Bigtable client being
// closed after sending off all the requests (>=1s delay should ensure the requests are already
// sent off when the client is closed).
// Note that the function doesn't manage the setup and teardown of resources.
func doReadRowsOpsCore(
	t *testing.T,
	clientID string,
	reqs []*testproxypb.ReadRowsRequest,
	closeCbtClientAfter *time.Duration) []*testproxypb.RowsResult {

	validateClientID(t, reqs, clientID)

	// Ask the CBT client to do ReadRows via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.RowsResult, len(reqs))
	for i := range reqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.ReadRows(context.Background(), reqs[i])
			fillResults(t, results, res, err, i)
		}(i)
	}
	if closeCbtClientAfter != nil {
		time.Sleep(*closeCbtClientAfter)
		closeCbtClient(t, clientID)
	}
	wg.Wait()

	return results
}

// doMutateRowOp is a simple wrapper of doMutateRowOps. It's useful when there is only one MutateRow
// operation to perform. A single result will be returned, where nil value indicates proxy
// failure (not client's).
func doMutateRowOp(
	t *testing.T,
	s *Server,
	req *testproxypb.MutateRowRequest,
	opts *clientOpts) *testproxypb.MutateRowResult {

	results := doMutateRowOps(t, s, []*testproxypb.MutateRowRequest{req}, opts)
	return results[0]
}

// doMutateRowOps performs MutateRow operations in parallel, using the test proxy requests `reqs`
// and the mock server `s`. Non-nil `opts` will override the default client settings including app
// profile id and timeout. The results will be returned, where the i-th result corresponds to the
// i-th request. nil element indicates proxy failure (not client's).
// Note that the function manages the setup and teardown of resources.
func doMutateRowOps(
	t *testing.T,
	s *Server,
	reqs []*testproxypb.MutateRowRequest,
	opts *clientOpts) []*testproxypb.MutateRowResult {

	clientID := reqs[0].GetClientId()
	setUp(t, s, clientID, opts)
	defer tearDown(t, s, clientID)

	return doMutateRowOpsCore(t, clientID, reqs, nil)
}

// doMutateRowOpsCore does the work of sending concurrent requests to test proxy and collecting the
// results, where the i-th result corresponds to the i-th request. nil element indicates proxy
// failure (not client's). Non-nil `closeCbtClientAfter` will trigger Cloud Bigtable client being
// closed after sending off all the requests (>=1s delay should ensure the requests are already
// sent off when the client is closed).
// Note that the function doesn't manage the setup and teardown of resources.
func doMutateRowOpsCore(
	t *testing.T,
	clientID string,
	reqs []*testproxypb.MutateRowRequest,
	closeCbtClientAfter *time.Duration) []*testproxypb.MutateRowResult {

	validateClientID(t, reqs, clientID)

	// Ask the CBT client to do MutateRow via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.MutateRowResult, len(reqs))
	for i := range reqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.MutateRow(context.Background(), reqs[i])
			fillResults(t, results, res, err, i)
		}(i)
	}
	if closeCbtClientAfter != nil {
		time.Sleep(*closeCbtClientAfter)
		closeCbtClient(t, clientID)
	}
	wg.Wait()

	return results
}

// doMutateRowsOp is a simple wrapper of doMutateRowsOps. It's useful when there is only one
// MutateRows operation to perform. A single result will be returned, where nil value indicates
// proxy failure (not client's).
func doMutateRowsOp(
	t *testing.T,
	s *Server,
	req *testproxypb.MutateRowsRequest,
	opts *clientOpts) *testproxypb.MutateRowsResult {

	results := doMutateRowsOps(t, s, []*testproxypb.MutateRowsRequest{req}, opts)
	return results[0]
}

// doMutateRowsOps performs MutateRows operations in parallel, using the test proxy requests `reqs`
// and the mock server `s`. Non-nil `opts` will override the default client settings including app
// profile id and timeout. The results will be returned, where the i-th result corresponds to the
// i-th request. nil element indicates proxy failure (not client's).
// Note that the function manages the setup and teardown of resources.
func doMutateRowsOps(
	t *testing.T,
	s *Server,
	reqs []*testproxypb.MutateRowsRequest,
	opts *clientOpts) []*testproxypb.MutateRowsResult {

	clientID := reqs[0].GetClientId()
	setUp(t, s, clientID, opts)
	defer tearDown(t, s, clientID)

	return doMutateRowsOpsCore(t, clientID, reqs, nil)
}

// doMutateRowsOpsCore does the work of sending concurrent requests to test proxy and collecting the
// results, where the i-th result corresponds to the i-th request. nil element indicates proxy
// failure (not client's). Non-nil `closeCbtClientAfter` will trigger Cloud Bigtable client being
// closed after sending off all the requests (>=1s delay should ensure the requests are already
// sent off when the client is closed).
// Note that the function doesn't manage the setup and teardown of resources.
func doMutateRowsOpsCore(
	t *testing.T,
	clientID string,
	reqs []*testproxypb.MutateRowsRequest,
	closeCbtClientAfter *time.Duration) []*testproxypb.MutateRowsResult {

	validateClientID(t, reqs, clientID)

	// Ask the CBT client to do MutateRows via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.MutateRowsResult, len(reqs))
	for i := range reqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.BulkMutateRows(context.Background(), reqs[i])
			fillResults(t, results, res, err, i)
		}(i)
	}
	if closeCbtClientAfter != nil {
		time.Sleep(*closeCbtClientAfter)
		closeCbtClient(t, clientID)
	}
	wg.Wait()

	return results
}

// doSampleRowKeysOp is a simple wrapper of doSampleRowKeysOps. It's useful when there is only one
// SampleRowKeys operation to perform. A single result will be returned, where nil value indicates
// proxy failure (not client's).
func doSampleRowKeysOp(
	t *testing.T,
	s *Server,
	req *testproxypb.SampleRowKeysRequest,
	opts *clientOpts) *testproxypb.SampleRowKeysResult {

	results := doSampleRowKeysOps(t, s, []*testproxypb.SampleRowKeysRequest{req}, opts)
	return results[0]
}

// doSampleRowKeysOps performs SampleRowKeys operations in parallel, using the test proxy requests
// `reqs` and the mock server `s`. Non-nil `opts` will override the default client settings
// including app profile id and timeout. The results will be returned, where the i-th result
// corresponds to the i-th request. nil element indicates proxy failure (not client's).
// Note that the function manages the setup and teardown of resources.
func doSampleRowKeysOps(
	t *testing.T,
	s *Server,
	reqs []*testproxypb.SampleRowKeysRequest,
	opts *clientOpts) []*testproxypb.SampleRowKeysResult {

	clientID := reqs[0].GetClientId()
	setUp(t, s, clientID, opts)
	defer tearDown(t, s, clientID)

	return doSampleRowKeysOpsCore(t, clientID, reqs, nil)
}

// doSampleRowKeysOpsCore does the work of sending concurrent requests to test proxy and collecting
// the results, where the i-th result corresponds to the i-th request. nil element indicates proxy
// failure (not client's). Non-nil `closeCbtClientAfter` will trigger Cloud Bigtable client being
// closed after sending off all the requests (>=1s delay should ensure the requests are already
// sent off when the client is closed).
// Note that the function doesn't manage the setup and teardown of resources.
func doSampleRowKeysOpsCore(
	t *testing.T,
	clientID string,
	reqs []*testproxypb.SampleRowKeysRequest,
	closeCbtClientAfter *time.Duration) []*testproxypb.SampleRowKeysResult {

	validateClientID(t, reqs, clientID)

	// Ask the CBT client to do SampleRowKeys via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.SampleRowKeysResult, len(reqs))
	for i := range reqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.SampleRowKeys(context.Background(), reqs[i])
			fillResults(t, results, res, err, i)
		}(i)
	}
	if closeCbtClientAfter != nil {
		time.Sleep(*closeCbtClientAfter)
		closeCbtClient(t, clientID)
	}
	wg.Wait()

	return results
}

// doCheckAndMutateRowOp is a simple wrapper of doCheckAndMutateRowOps. It's useful when there is
// only one CheckAndMutateRow operation to perform. A single result will be returned, where nil
// value indicates proxy failure (not client's).
func doCheckAndMutateRowOp(
	t *testing.T,
	s *Server,
	req *testproxypb.CheckAndMutateRowRequest,
	opts *clientOpts) *testproxypb.CheckAndMutateRowResult {

	results := doCheckAndMutateRowOps(t, s, []*testproxypb.CheckAndMutateRowRequest{req}, opts)
	return results[0]
}

// doCheckAndMutateRowOps performs CheckAndMutateRow operations in parallel, using the test proxy
// requests `reqs` and the mock server `s`. Non-nil `opts` will override the default client settings
// including app profile id and timeout. The results will be returned, where the i-th result
// corresponds to the i-th request. nil element indicates proxy failure (not client's).
// Note that the function manages the setup and teardown of resources.
func doCheckAndMutateRowOps(
	t *testing.T,
	s *Server,
	reqs []*testproxypb.CheckAndMutateRowRequest,
	opts *clientOpts) []*testproxypb.CheckAndMutateRowResult {

	clientID := reqs[0].GetClientId()
	setUp(t, s, clientID, opts)
	defer tearDown(t, s, clientID)

	return doCheckAndMutateRowOpsCore(t, clientID, reqs, nil)
}

// doCheckAndMutateRowOpsCore does the work of sending concurrent requests to test proxy and
// collecting the results, where the i-th result corresponds to the i-th request. nil element
// indicates proxy failure (not client's). Non-nil `closeCbtClientAfter` will trigger Cloud Bigtable
// client being closed after sending off all the requests (>=1s delay should ensure the requests are
// already sent off when the client is closed).
// Note that the function doesn't manage the setup and teardown of resources.
func doCheckAndMutateRowOpsCore(
	t *testing.T,
	clientID string,
	reqs []*testproxypb.CheckAndMutateRowRequest,
	closeCbtClientAfter *time.Duration) []*testproxypb.CheckAndMutateRowResult {

	validateClientID(t, reqs, clientID)

	// Ask the CBT client to do CheckAndMutateRow via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.CheckAndMutateRowResult, len(reqs))
	for i := range reqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.CheckAndMutateRow(context.Background(), reqs[i])
			fillResults(t, results, res, err, i)
		}(i)
	}
	if closeCbtClientAfter != nil {
		time.Sleep(*closeCbtClientAfter)
		closeCbtClient(t, clientID)
	}
	wg.Wait()

	return results
}

// doReadModifyWriteRowOp is a simple wrapper of doReadModifyWriteRowOps. It's useful when there is
// only one ReadModifyWriteRow operation to perform. A single result will be returned, where nil
// value indicates proxy failure (not client's).
func doReadModifyWriteRowOp(
	t *testing.T,
	s *Server,
	req *testproxypb.ReadModifyWriteRowRequest,
	opts *clientOpts) *testproxypb.RowResult {

	results := doReadModifyWriteRowOps(t, s, []*testproxypb.ReadModifyWriteRowRequest{req}, opts)
	return results[0]
}

// doReadModifyWriteRowOps performs ReadModifyWriteRow operations in parallel, using the test proxy
// requests `reqs` and the mock server `s`. Non-nil `opts` will override the default client settings
// including app profile id and timeout. The results will be returned, where the i-th result
// corresponds to the i-th request. nil element indicates proxy failure (not client's).
// Note that the function manages the setup and teardown of resources.
func doReadModifyWriteRowOps(
	t *testing.T,
	s *Server,
	reqs []*testproxypb.ReadModifyWriteRowRequest,
	opts *clientOpts) []*testproxypb.RowResult {

	clientID := reqs[0].GetClientId()
	setUp(t, s, clientID, opts)
	defer tearDown(t, s, clientID)

	return doReadModifyWriteRowOpsCore(t, clientID, reqs, nil)
}

// doReadModifyWriteRowOpsCore does the work of sending concurrent requests to test proxy and
// collecting the results, where the i-th result corresponds to the i-th request. nil element
// indicates proxy failure (not client's). Non-nil `closeCbtClientAfter` will trigger Cloud Bigtable
// client being closed after sending off all the requests (>=1s delay should ensure the requests are
// already sent off when the client is closed).
// Note that the function doesn't manage the setup and teardown of resources.
func doReadModifyWriteRowOpsCore(
	t *testing.T,
	clientID string,
	reqs []*testproxypb.ReadModifyWriteRowRequest,
	closeCbtClientAfter *time.Duration) []*testproxypb.RowResult {

	validateClientID(t, reqs, clientID)

	// Ask the CBT client to do ReadModifyWriteRow via the test proxy
	var wg sync.WaitGroup
	results := make([]*testproxypb.RowResult, len(reqs))
	for i := range reqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := testProxyClient.ReadModifyWriteRow(context.Background(), reqs[i])
			fillResults(t, results, res, err, i)
		}(i)
	}
	if closeCbtClientAfter != nil {
		time.Sleep(*closeCbtClientAfter)
		closeCbtClient(t, clientID)
	}
	wg.Wait()

	return results
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
		loggedReq, more := <-records
		if !more {
			break
		}

		ts := loggedReq.GetTs().UnixMilli()
		if minTsMs > ts {
			minTsMs = ts
		}
		if maxTsMs < ts {
			maxTsMs = ts
		}
	}
	t.Logf("The requests were received within %dms", maxTsMs-minTsMs)
	assert.Less(t, maxTsMs-minTsMs, int64(periodMillisec))
}

func getClientFeatureFlags(md metadata.MD) (ff *btpb.FeatureFlags, err error) {
	featuresMd := md["bigtable-features"]
	if len(featuresMd) != 1 {
		err = fmt.Errorf("bigtable-features metadata missing")
		return ff, err
	}

	featuresBytes, err := base64.URLEncoding.DecodeString(featuresMd[0])

	if err != nil {
		return ff, err
	}

	ff = &btpb.FeatureFlags{}
	err = proto.Unmarshal(featuresBytes, ff)

	return ff, err
}
