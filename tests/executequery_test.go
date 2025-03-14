// Copyright 2024 Google LLC
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

//go:build !emulator
// +build !emulator

package tests

import (
	"testing"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/testing/protocmp"
)

// Tests that a query will run successfully when receiving a response with no rows
func TestExecuteQuery_EmptyResponse(t *testing.T) {
	// 1. Instantiate the mock server
	server := initMockServer(t)
	prepareRecorder := make(chan *prepareQueryReqRecord, 1)
	server.PrepareQueryFn = mockPrepareQueryFn(prepareRecorder,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(column("test", strType()))),
		},
	)
	executeRecorder := make(chan *executeQueryReqRecord, 1)
	server.ExecuteQueryFn = mockExecuteQueryFn(executeRecorder, &executeQueryAction{
		endOfStream: true,
	})
	// 2. Build the request to test proxy
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	// 3. Perform the operation via test proxy
	res := doExecuteQueryOp(t, server, &req, nil)
	// 4. Verify the read succeeds, gets the expected metadata, and the client sends the requests properly
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(column("test", strType())), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 0)

	expectedPrepareReq := &btpb.PrepareQueryRequest{
		InstanceName: instanceName,
		Query:        "SELECT * FROM table",
	}
	origPrepareReq := <-prepareRecorder
	if diff := cmp.Diff(expectedPrepareReq, origPrepareReq.req, protocmp.Transform(), protocmp.IgnoreEmptyMessages()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}
	expectedExecuteReq := &btpb.ExecuteQueryRequest{
		InstanceName:  instanceName,
		PreparedQuery: []byte("foo"),
	}
	origExecuteReq := <-executeRecorder
	if diff := cmp.Diff(expectedExecuteReq, origExecuteReq.req, protocmp.Transform(), protocmp.IgnoreEmptyMessages()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}
}

// Tests that a query will run successfully when receiving a simple response
func TestExecuteQuery_SingleSimpleRow(t *testing.T) {
	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(column("test", strType()))),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    partialResultSet("token", strValue("foo")),
			endOfStream: true,
		})
	// 2. Build the request to test proxy
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	// 3. Perform the operation via test proxy
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	// 4. Verify the read succeeds, gets the expected metadata & data, and the client sends the request properly
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(column("test", strType())), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assert.True(t, cmp.Equal(res.Rows[0], testProxyRow(strValue("foo")), protocmp.Transform()))
}
