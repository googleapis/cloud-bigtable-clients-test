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
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
)

func toGoMap(p *btpb.Value) map[*btpb.Value]*btpb.Value {
	var m = make(map[*btpb.Value]*btpb.Value)
	if p.GetKind() == nil {
		return nil
	} else {
		arr := p.GetArrayValue()
		for _, kvArr := range arr.Values {
			m[kvArr.GetArrayValue().Values[0]] = kvArr.GetArrayValue().Values[1]
		}
	}
	return m
}

// We don't use cmp directly because we need special handling for maps to ignore ordering.
// This is because maps are represented as Array values (which are ordered) but clients will
// not preserve that ordering when they convert to native maps.
// Note that this will not currently handle nested map ordering correctly if we ever support
// nested maps
func assertRowEqual(t *testing.T, want *testproxypb.SqlRow, got *testproxypb.SqlRow, metadata *testproxypb.ResultSetMetadata) {
	var equal = len(metadata.Columns) == len(want.Values) && len(want.Values) == len(got.Values)
	if !equal {
		println("Lengths do not match. Full diff (which may be incorrectly show diffs based on map ordering):")
		println(cmp.Diff(want, got, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)))
	}
	for i := 0; i < len(metadata.Columns); i++ {
		col := metadata.Columns[i]
		var diff = ""
		switch col.Type.GetKind().(type) {
		case *btpb.Type_MapType:
			// We don't want to enforce map entry order, but we still want to make sure key-value ordering is correct
			// and that ordering of nested fields is correct. So this first converts to a go map and then compares those maps.
			// In order to use protoCmp on a go map with proto messages as keys the recommended approach is to convert it back
			// to a slice using SortMaps, and then use protocmp.Transform. Transform does not work on map keys
			diff = cmp.Diff(toGoMap(want.Values[i]), toGoMap(got.Values[i]), cmpopts.SortMaps(func(x, y *btpb.Value) bool {
				// We only care that the order is consistent here.
				return x.String() < y.String()
			}), protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001))
			break
		default:
			diff = cmp.Diff(want.Values[i], got.Values[i], protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001))
			break
		}
		if diff != "" {
			equal = false
			fmt.Println("Value at column ", i, " does not match. Diff:")
			println(diff)
		}
	}
	assert.True(t, equal)
}

// Check that res has an error message and it contains one of the partial messages in `messages`.
// This allows us to support different messages from different client languages.
func assertErrorIn(t *testing.T, res *testproxypb.ExecuteQueryResult, messages []string) {
	msgLower := strings.ToLower(res.GetStatus().GetMessage())
	foundMatch := false
	for _, m := range messages {
		adjustedExpectation := strings.ToLower(m)
		if strings.Contains(msgLower, adjustedExpectation) {
			foundMatch = true
		}
	}
	if !foundMatch {
		assert.Fail(t, "Got unexpected message: "+res.GetStatus().GetMessage())
	}
}

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
			response:    partialResultSet("token", strVal("foo")),
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
	// 4. Verify the read succeeds, gets the expected metadata & data, and the client sends the request properly
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(column("test", strType())), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(strVal("foo")), res.Rows[0], res.Metadata)
}

// Tests that a query runs successfully for various data types
func TestExecuteQuery_TypesTest(t *testing.T) {
	// 1. Instantiate the mock server with diverse column types
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("strCol", strType()),
		column("bytesCol", bytesType()),
		column("int64Col", int64Type()),
		column("boolCol", boolType()),
		column("float32Col", float32Type()),
		column("float64Col", float64Type()),
		column("dateCol", dateType()),
		column("tsCol", timestampType()),
		column("structCol", structType(
			structField("strField", strType()),
			structField("intField", int64Type()),
			structField("arrField", arrayType(bytesType())))),
		column("arrayCol", arrayType(bytesType())),
		// simple map
		column("mapCol", mapType(bytesType(), strType())),
		// historical map
		column("historicalMap", mapType(
			bytesType(),
			arrayType(structType(
				structField("timestamp", timestampType()),
				structField("value", bytesType()))))),
	}
	expectedValues := []*btpb.Value{
		strVal("strVal"),
		bytesVal([]byte("bytesVal")),
		intVal(10),
		boolVal(true),
		floatVal(1.2),
		floatVal(1.3),
		dateVal(2024, 9, 1),
		timestampVal(2000, 1000),
		structVal(strVal("field"), intVal(100), arrayVal(bytesVal([]byte("foo")), bytesVal([]byte("bar")))),
		arrayVal(bytesVal([]byte("elem"))),
		mapVal(mapEntry(bytesVal([]byte("key")), strVal("val"))),
		mapVal(mapEntry(bytesVal([]byte("key")), arrayVal(
			structVal(timestampVal(10000, 1000), bytesVal([]byte("val1"))),
			structVal(timestampVal(20000, 1000), bytesVal([]byte("val2")))))),
	}
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(columns...)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: partialResultSet(
				"token",
				expectedValues...,
			),
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
	// 4. Verify the read succeeds and gets the expected metadata & data
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 12)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(expectedValues...), res.Rows[0], res.Metadata)
}

// Tests that a query runs successfully when receiving NULL values for various data types
func TestExecuteQuery_NullsTest(t *testing.T) {
	// 1. Instantiate the mock server with diverse column types
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("strCol", strType()),
		column("bytesCol", bytesType()),
		column("int64Col", int64Type()),
		column("boolCol", boolType()),
		column("float32Col", float32Type()),
		column("float64Col", float64Type()),
		column("dateCol", dateType()),
		column("tsCol", timestampType()),
		column("structCol", structType(
			structField("strField", strType()),
			structField("intField", int64Type()),
			structField("arrField", arrayType(bytesType())))),
		column("arrayCol", arrayType(bytesType())),
		// simple map
		column("mapCol", mapType(bytesType(), strType())),
		// historical map
		column("historicalMap", mapType(
			bytesType(),
			arrayType(structType(
				structField("timestamp", timestampType()),
				structField("value", bytesType()))))),
	}
	// Expect NULL for all columns
	expectedValues := []*btpb.Value{
		nullVal(), nullVal(), nullVal(), nullVal(), nullVal(), nullVal(),
		nullVal(), nullVal(), nullVal(), nullVal(), nullVal(), nullVal(),
	}
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(columns...)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: partialResultSet(
				"token",
				expectedValues...,
			),
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
	// 4. Verify the read succeeds and gets the expected metadata & data
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 12)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(expectedValues...), res.Rows[0], res.Metadata)
}

// Tests that a query runs successfully when receiving nested NULL values within complex types
func TestExecuteQuery_NestedNullsTest(t *testing.T) {
	// 1. Instantiate the mock server with complex column types (struct, array, map)
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("structCol", structType(
			structField("strField", strType()),
			structField("intField", int64Type()),
			structField("arrField", arrayType(bytesType())))),
		column("arrayCol", arrayType(int64Type())),
		// simple map
		column("mapCol", mapType(bytesType(), strType())),
	}
	// Expect values with NULLs nested inside structs, arrays, and maps
	expectedValues := []*btpb.Value{
		structVal(strVal("foo"), nullVal(), arrayVal(bytesVal([]byte("foo")), nullVal())),
		arrayVal(intVal(100), nullVal(), intVal(200), nullVal()),
		mapVal(mapEntry(nullVal(), strVal("foo")), mapEntry(bytesVal([]byte("bar")), nullVal())),
	}
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(columns...)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: partialResultSet(
				"token",
				expectedValues...,
			),
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
	// 4. Verify the read succeeds and gets the expected metadata & data
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 3)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(expectedValues...), res.Rows[0], res.Metadata)
}

// Tests that a query runs successfully when receiving a map with duplicate keys
func TestExecuteQuery_MapAllowsDuplicateKey(t *testing.T) {
	// 1. Instantiate the mock server with a map column type
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("mapCol", mapType(bytesType(), strType())),
	}
	// Mock response contains a map with the same key ("foo") twice
	rawValues := []*btpb.Value{
		mapVal(mapEntry(bytesVal([]byte("foo")), strVal("foo")), mapEntry(bytesVal([]byte("foo")), strVal("bar"))),
	}
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(columns...)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: partialResultSet(
				"token",
				rawValues...,
			),
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
	// 4. Verify the read succeeds and gets the expected metadata & data (last value wins for duplicate key)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	// For values with duplicate keys, the last value should win.
	assertRowEqual(t, testProxyRow(mapVal(mapEntry(bytesVal([]byte("foo")), strVal("bar")))), res.Rows[0], res.Metadata)
}

// Tests that a query with parameters runs successfully
func TestExecuteQuery_QueryParams(t *testing.T) {
	// 1. Instantiate the mock server with columns matching the expected output of the parameterized query
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("strCol", strType()),
		column("bytesCol", bytesType()),
		column("intCol", int64Type()),
		column("doubleCol", float64Type()),
		column("floatCol", float32Type()),
		column("boolCol", boolType()),
		column("tsCol", timestampType()),
		column("dateCol", dateType()),
		column("byteArrayCol", arrayType(bytesType())),
		column("stringArrayCol", arrayType(strType())),
		column("intArrayCol", arrayType(int64Type())),
		column("floatArrayCol", arrayType(float32Type())),
		column("doubleArrayCol", arrayType(float64Type())),
		column("boolArrayCol", arrayType(boolType())),
		column("tsArrayCol", arrayType(timestampType())),
		column("dateArrayParam", arrayType(dateType())),
	}
	// Define the expected row values (which should match the parameters sent)
	expectedValues := []*btpb.Value{
		strVal("s"),
		bytesVal([]byte("b")),
		intVal(200),
		floatVal(1.55),
		floatVal(1.4),
		boolVal(true),
		timestampVal(1000000, 1000),
		dateVal(2024, 9, 1),
		arrayVal(bytesVal([]byte("foo")), nullVal()),
		arrayVal(strVal("foo"), nullVal()),
		arrayVal(intVal(123), nullVal()),
		arrayVal(floatVal(1.23), nullVal()),
		arrayVal(floatVal(4.56), nullVal()),
		arrayVal(boolVal(true), nullVal()),
		arrayVal(timestampVal(100000000, 2000), nullVal()),
		arrayVal(dateVal(2024, 9, 2), nullVal()),
	}

	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(columns...)),
		},
	)

	recorder := make(chan *executeQueryReqRecord, 1)
	server.ExecuteQueryFn = mockExecuteQueryFn(recorder,
		&executeQueryAction{
			response: partialResultSet(
				"token",
				expectedValues...,
			),
			endOfStream: true,
		})

	// 2. Build the request to test proxy, including the query and parameters
	params := map[string]*btpb.Value{
		"stringParam":      strValWithType("s"),
		"bytesParam":       bytesValWithType([]byte("b")),
		"int64Param":       intValWithType(200),
		"doubleParam":      float64ValWithType(1.55),
		"floatParam":       float32ValWithType(1.4),
		"boolParam":        boolValWithType(true),
		"tsParam":          timestampValWithType(1000000, 1000),
		"dateParam":        dateValWithType(2024, 9, 1),
		"byteArrayParam":   arrayValWithType(bytesType(), bytesVal([]byte("foo")), nullVal()),
		"stringArrayParam": arrayValWithType(strType(), strVal("foo"), nullVal()),
		"intArrayParam":    arrayValWithType(int64Type(), intVal(123), nullVal()),
		"floatArrayParam":  arrayValWithType(float32Type(), floatVal(1.23), nullVal()),
		"doubleArrayParam": arrayValWithType(float64Type(), floatVal(4.56), nullVal()),
		"boolArrayParam":   arrayValWithType(boolType(), boolVal(true), nullVal()),
		"tsArrayParam":     arrayValWithType(timestampType(), timestampVal(100000000, 2000), nullVal()),
		"dateArrayParam":   arrayValWithType(dateType(), dateVal(2024, 9, 2), nullVal()),
	}
	proxyReq := &btpb.ExecuteQueryRequest{
		InstanceName: instanceName,
		Query: `SELECT @stringParam AS strCol, @bytesParam as bytesCol, @int64Param AS intCol, @doubleParam AS doubleCol, 
				@floatParam AS floatCol, @boolParam AS boolCol, @tsParam AS tsCol, @dateParam AS dateCol, 
				@byteArrayParam AS byteArrayCol, @stringArrayParam AS stringArrayCol, @intArrayParam AS intArrayCol, 
				@floatArrayParam AS floatArrayCol, @doubleArrayParam AS doubleArrayCol, @boolArrayParam AS boolArrayCol, 
				@tsArrayParam AS tsArrayCol, @dateArrayParam AS dateArrayCol`,
		Params: params,
	}
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request:  proxyReq,
	}
	expectedReq := &btpb.ExecuteQueryRequest{
		InstanceName:  instanceName,
		PreparedQuery: []byte("foo"),
		Params:        params,
	}

	// 3. Perform the operation via test proxy
	res := doExecuteQueryOp(t, server, &req, nil)
	// 4. Verify the read succeeds, the request was sent correctly, and gets the expected metadata & data
	checkResultOkStatus(t, res)
	loggedReq := <-recorder
	assert.True(t, cmp.Equal(loggedReq.req, expectedReq, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)))
	assert.Equal(t, len(res.Metadata.Columns), 16)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(expectedValues...), res.Rows[0], res.Metadata)
}

// Tests that a query runs successfully when results are chunked within a single response stream
func TestExecuteQuery_ChunkingTest(t *testing.T) {
	// 1. Instantiate the mock server with columns containing potentially large data
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("bytesCol", bytesType()),
		column("arrayCol", arrayType(bytesType())),
		// historical map
		column("historicalMap", mapType(
			bytesType(),
			arrayType(structType(
				structField("timestamp", timestampType()),
				structField("value", bytesType()))))),
	}
	// Define expected values for two rows
	expectedValues := []*btpb.Value{
		// row 1
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
		// row 2
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
	}
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(columns...)),
		},
	)
	// Mock ExecuteQuery to return results chunked across 3 stream messages
	results := chunkedPartialResultSet(3, "token", expectedValues...)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    results[0],
			endOfStream: false,
		},
		&executeQueryAction{
			response:    results[1],
			endOfStream: false,
		},
		&executeQueryAction{
			response:    results[2],
			endOfStream: true, // Final chunk
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
	// 4. Verify the read succeeds and reconstructs the expected metadata & data from chunks
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 3)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 2)
	assertRowEqual(t, testProxyRow(expectedValues[0:3]...), res.Rows[0], res.Metadata)
	assertRowEqual(t, testProxyRow(expectedValues[3:6]...), res.Rows[1], res.Metadata)
}

// Tests that a query runs successfully when results are split across multiple response streams (batches)
func TestExecuteQuery_BatchesTest(t *testing.T) {
	// 1. Instantiate the mock server with columns containing potentially large data
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("bytesCol", bytesType()),
		column("arrayCol", arrayType(bytesType())),
		// historical map
		column("historicalMap", mapType(
			bytesType(),
			arrayType(structType(
				structField("timestamp", timestampType()),
				structField("value", bytesType()))))),
	}
	// Define expected values for four rows
	expectedValues := []*btpb.Value{
		// row 1
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
		// row 2
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
		// row 3
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
		// row 4
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
	}
	// Results chunked into two batches
	batch1 := chunkedPartialResultSet(2, "token1", expectedValues[0:6]...)  // First 2 rows, with resume token "token1"
	batch2 := chunkedPartialResultSet(2, "token2", expectedValues[6:12]...) // Next 2 rows, with final token "token2"
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(columns...)),
		},
	)
	// Return all of the batches
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{ // Batch 1, Chunk 1
			response:    batch1[0],
			endOfStream: false,
		},
		&executeQueryAction{ // Batch 1, Chunk 2 (contains resume token "token1")
			response:    batch1[1],
			endOfStream: false,
		},
		// Assume client sends new request with "token1"
		&executeQueryAction{ // Batch 2, Chunk 1
			response:    batch2[0],
			endOfStream: false,
		},
		&executeQueryAction{ // Batch 2, Chunk 2 (contains final token "token2")
			response:    batch2[1],
			endOfStream: true,
		})
	// 2. Build the initial request to test proxy
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	// 3. Perform the operation via test proxy (it should handle batching/resumption)
	res := doExecuteQueryOp(t, server, &req, nil)
	// 4. Verify the read succeeds and reconstructs all expected metadata & data from batches
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 3)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 4)
	assertRowEqual(t, testProxyRow(expectedValues[0:3]...), res.Rows[0], res.Metadata)
	assertRowEqual(t, testProxyRow(expectedValues[3:6]...), res.Rows[1], res.Metadata)
	assertRowEqual(t, testProxyRow(expectedValues[6:9]...), res.Rows[2], res.Metadata)
	assertRowEqual(t, testProxyRow(expectedValues[9:12]...), res.Rows[3], res.Metadata)
}

// Tests that the operation fails if PrepareQuery returns empty metadata
func TestExecuteQuery_FailsOnEmptyMetadata(t *testing.T) {
	// 1. Instantiate the mock server to return empty metadata from PrepareQuery
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md()), // Empty metadata
		},
	)

	// Python doesn't check metadata is valid until yielding data (first token) so
	// we need to return something here
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    partialResultSet("token", bytesVal([]byte("foo")), strVal("s"), bytesVal([]byte("bar"))),
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
	// 4. Verify the operation fails with an appropriate error
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assertErrorIn(t, res, []string{
		// Java error message
		"columns cannot be empty",
		// Python error message
		"Invalid empty ResultSetMetadata received",
	})
}

// Tests that the operation fails if ExecuteQuery returns metadata unexpectedly
func TestExecuteQuery_FailsOnExecuteQueryMetadata(t *testing.T) {
	// 1. Instantiate the mock server to return metadata during ExecuteQuery (which is invalid)
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(column("bytesCol", bytesType()))),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: &btpb.ExecuteQueryResponse{ // Send metadata instead of results
				Response: &btpb.ExecuteQueryResponse_Metadata{
					Metadata: md(column("bytesCol", bytesType())),
				},
			},
			endOfStream: false,
		},
	)
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
	// 4. Verify the operation fails with an appropriate error
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assertErrorIn(t, res, []string{
		// Java error message
		"Expected results response, but received: METADATA",
		// Python error message
		"InvalidExecuteQueryResponse",
	})
}

// Tests that the operation fails if PrepareQuery returns metadata with an invalid column type
func TestExecuteQuery_FailsOnInvalidType(t *testing.T) {
	// 1. Instantiate the mock server to return metadata with an empty/invalid type
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				column("test", bytesType()),
				column("invalid", &btpb.Type{}), // Invalid empty type
			)),
		},
	)
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
	// 4. Verify the operation fails with an appropriate error
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assertErrorIn(t, res, []string{
		// Java error message
		"Column type cannot be empty",
		// Python error message
		"Unrecognized response data type",
	})
}

// Tests that the operation fails if the response stream ends with an incomplete row
func TestExecuteQuery_FailsOnNotEnoughData(t *testing.T) {
	// 1. Instantiate the mock server to return partial row data before ending the stream
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				column("foo", bytesType()),
				column("bar", strType()), // Expects 2 values per row
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    partialResultSet("token", bytesVal([]byte("foo"))), // Only 1 value provided with token
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
	// 4. Verify the operation fails with an appropriate error about incomplete data
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assertErrorIn(t, res, []string{
		// Java error message
		"Incomplete row received",
		// Python error message
		"InvalidExecuteQueryResponse",
	})
}

// Tests that the operation fails if the response stream ends with an incomplete row, even after complete rows
func TestExecuteQuery_FailsOnNotEnoughDataWithCompleteRows(t *testing.T) {
	// 1. Instantiate the mock server to return a complete row, then partial row data
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				column("foo", bytesType()),
				column("bar", strType()), // Expects 2 values per row
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			// One complete row (2 values) + start of next row (1 value)
			response:    partialResultSet("token", bytesVal([]byte("foo")), strVal("s"), bytesVal([]byte("bar"))),
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
	// 4. Verify the operation fails with an appropriate error about incomplete data
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assertErrorIn(t, res, []string{
		// Java error message
		"Incomplete row received",
		// Python error message
		"InvalidExecuteQueryResponse",
	})
}

// Tests that the operation fails if received data type mismatches the metadata type
func TestExecuteQuery_FailsOnTypeMismatch(t *testing.T) {
	// 1. Instantiate the mock server to return data with a type mismatch
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				column("foo", bytesType()),
				column("bar", strType()), // Expects string for 2nd column
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    partialResultSet("token", bytesVal([]byte("foo")), strVal("s"), bytesVal([]byte("bar")), intVal(42)), // Sends int where string expected
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
	// 4. Verify the operation fails with a type mismatch error
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assertErrorIn(t, res, []string{
		// Java error message
		"Value kind must be STRING_VALUE for columns of type: STRING",
		// Python error message
		"InvalidExecuteQueryResponse",
	})
}

// Tests that the operation fails on a type mismatch within a map value
func TestExecuteQuery_FailsOnTypeMismatchWithinMap(t *testing.T) {
	// 1. Instantiate the mock server to return a map with an incorrect value type
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				column("foo", mapType(strType(), int64Type())), // Expects map<string, int64>
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			// Sends map<string, string> instead of map<string, int64>
			response:    partialResultSet("token", mapVal(mapEntry(strVal("s"), strVal("wrong")))),
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
	// 4. Verify the operation fails with a type mismatch error
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assertErrorIn(t, res, []string{
		// Java error message
		"Value kind must be INT_VALUE for columns of type: INT64",
		// Python error message
		"InvalidExecuteQueryResponse",
	})
}

// Tests that the operation fails on a type mismatch within an array value
func TestExecuteQuery_FailsOnTypeMismatchWithinArray(t *testing.T) {
	// 1. Instantiate the mock server to return an array with an incorrect element type
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				column("arr", arrayType(strType())), // Expects array<string>
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			// Sends array<string, int> instead of array<string>
			response:    partialResultSet("token", arrayVal(strVal("foo"), intVal(1))),
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
	// 4. Verify the operation fails with a type mismatch error
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assertErrorIn(t, res, []string{
		// Java error message
		"Value kind must be STRING_VALUE for columns of type: STRING",
		// Python error message
		"InvalidExecuteQueryResponse",
	})
}

// Tests that the operation fails on a type mismatch within a struct value
func TestExecuteQuery_FailsOnTypeMismatchWithinStruct(t *testing.T) {
	// 1. Instantiate the mock server to return a struct with an incorrect field type
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				// Expects struct<s:string, i:int64>
				column("struct", structType(structField("s", strType()), structField("i", int64Type()))),
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			// Sends struct<string, string> instead of struct<string, int64>
			response:    partialResultSet("token", structVal(strVal("foo"), strVal("bar"))),
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
	// 4. Verify the operation fails with a type mismatch error
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assertErrorIn(t, res, []string{
		// Java error message
		"Value kind must be INT_VALUE for columns of type: INT64",
		// Python error message
		"InvalidExecuteQueryResponse",
	})
}

// Tests that the operation fails if a struct value is missing a field defined in the metadata
func TestExecuteQuery_FailsOnStructMissingField(t *testing.T) {
	// 1. Instantiate the mock server to return a struct value with fewer fields than expected
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				// Expects struct<s:string, i:int64> (2 fields)
				column("struct", structType(structField("s", strType()), structField("i", int64Type()))),
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    partialResultSet("token", structVal(strVal("foo"))), // Sends only 1 field
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
	// 4. Verify the operation fails with an error about missing fields/index out of bounds
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assertErrorIn(t, res, []string{
		// Java error message
		"Unexpected malformed struct data",
		// Python error message
		"InvalidExecuteQueryResponse",
	})
}

// Tests that a query runs successfully with struct metadata that has no field names
func TestExecuteQuery_StructWithNoColumnNames(t *testing.T) {
	// 1. Instantiate the mock server with struct metadata lacking field names
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				// Struct fields have types but no names
				column("struct", structType(namelessStructField(strType()), namelessStructField(int64Type()))),
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    partialResultSet("token", structVal(strVal("foo"), intVal(100)), structVal(strVal("bar"), intVal(101))),
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
	// 4. Verify the read succeeds and gets the expected metadata & data
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata,
		testProxyMd(column("struct",
			structType(namelessStructField(strType()), namelessStructField(int64Type())))),
		protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 2)
	assertRowEqual(t, testProxyRow(structVal(strVal("foo"), intVal(100))), res.Rows[0], res.Metadata)
	assertRowEqual(t, testProxyRow(structVal(strVal("bar"), intVal(101))), res.Rows[1], res.Metadata)
}

// Tests that a query runs successfully with struct metadata that has duplicate field names
func TestExecuteQuery_StructWithDuplicateColumnNames(t *testing.T) {
	// 1. Instantiate the mock server with struct metadata having duplicate field names
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				// Struct fields have the same name "foo"
				column("struct", structType(structField("foo", strType()), structField("foo", int64Type()))),
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    partialResultSet("token", structVal(strVal("foo"), intVal(100)), structVal(strVal("bar"), intVal(101))),
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
	// 4. Verify the read succeeds and gets the expected metadata & data
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata,
		testProxyMd(column("struct",
			structType(structField("foo", strType()), structField("foo", int64Type())))),
		protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 2)
	assertRowEqual(t, testProxyRow(structVal(strVal("foo"), intVal(100))), res.Rows[0], res.Metadata)
	assertRowEqual(t, testProxyRow(structVal(strVal("bar"), intVal(101))), res.Rows[1], res.Metadata)
}

// Tests that the operation fails if the final successful stream response lacks a resume token
// (This might be specific to internal proxy logic or expectations)
func TestExecuteQuery_FailsOnSuccesfulStreamWithNoToken(t *testing.T) {
	// 1. Instantiate the mock server to return a final response with no resume token
	server := initMockServer(t)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				column("intCol", int64Type()),
				column("strCol", strType()),
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    partialResultSet("", intVal(100), strVal("foo")), // Empty resume token ""
			endOfStream: true,                                             // Indicates end of stream
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
	// 4. Verify the operation fails (assuming the proxy expects a non-empty token on success)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
}

// Tests that appropriate request headers (routing, client info) are set for PrepareQuery and ExecuteQuery calls
func TestExecuteQuery_HeadersAreSet(t *testing.T) {
	// 1. Instantiate the mock server and recorders for requests and metadata
	server := initMockServer(t)
	prepareRecorder := make(chan *prepareQueryReqRecord, 1)
	prepareMdRecorder := make(chan metadata.MD, 1)
	server.PrepareQueryFn = mockPrepareQueryFnWithMetadata(prepareRecorder, prepareMdRecorder,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				column("strCol", strType()),
				column("intCol", int64Type()),
			)),
		},
	)
	executeRecorder := make(chan *executeQueryReqRecord, 1)
	executeMdRecorder := make(chan metadata.MD, 1)
	server.ExecuteQueryFn = mockExecuteQueryFnWithMetadataSimple(executeRecorder, executeMdRecorder,
		&executeQueryAction{
			response:    partialResultSet("token", strVal("foo"), intVal(100)),
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
	// Set client options including an app profile ID
	appProfileId := "headers-test"
	opts := clientOpts{
		profile: appProfileId,
	}
	// 3. Perform the operation via test proxy with client options
	res := doExecuteQueryOp(t, server, &req, &opts)
	// 4. Verify the operation succeeds, gets expected data, and sent correct headers
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 2)
	assert.True(t, cmp.Equal(res.Metadata,
		testProxyMd(column("strCol", strType()),
			column("intCol", int64Type())),
		protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(strVal("foo"), intVal(100)), res.Rows[0], res.Metadata)

	// Check the request headers in the prepare metadata
	prepareHeaders := <-prepareMdRecorder
	if len(prepareHeaders["user-agent"]) == 0 && len(prepareHeaders["x-goog-api-client"]) == 0 {
		assert.Fail(t, "Client info is missing in the request header")
	}
	// Check RLS headers
	prepareResource := prepareHeaders["x-goog-request-params"][0]
	if !strings.Contains(prepareResource, instanceName) && !strings.Contains(prepareResource, url.QueryEscape(instanceName)) {
		assert.Fail(t, "Resource info is missing in the request header")
	}
	assert.Contains(t, prepareResource, appProfileId) // Check for app profile

	// Check the request headers in the execute metadata
	executeHeaders := <-executeMdRecorder
	if len(executeHeaders["user-agent"]) == 0 && len(executeHeaders["x-goog-api-client"]) == 0 {
		assert.Fail(t, "Client info is missing in the request header")
	}
	// Check RLS headers
	executeResource := executeHeaders["x-goog-request-params"][0]
	if !strings.Contains(executeResource, instanceName) && !strings.Contains(executeResource, url.QueryEscape(instanceName)) {
		assert.Fail(t, "Resource info is missing in the request header")
	}
	assert.Contains(t, executeResource, appProfileId) // Check for app profile
}

// Tests that the ExecuteQuery RPC respects the client-specified deadline/timeout
func TestExecuteQuery_ExecuteQueryRespectsDeadline(t *testing.T) {
	// 1. Instantiate the mock server with a delay in ExecuteQuery longer than the client timeout
	server := initMockServer(t)
	recorder := make(chan *executeQueryReqRecord, 1)
	server.PrepareQueryFn = mockPrepareQueryFn(nil,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				column("test", strType()),
			)),
		},
	)
	server.ExecuteQueryFn = mockExecuteQueryFn(recorder,
		&executeQueryAction{
			delayStr:    "10s", // Server delay
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
	// Set client options with a timeout shorter than the server delay
	opts := &clientOpts{
		timeout: &durationpb.Duration{Seconds: 2}, // Client timeout
	}
	// 3. Perform the operation via test proxy with timeout options
	res := doExecuteQueryOp(t, server, &req, opts)
	// 4. Verify the operation times out and returns a DeadlineExceeded error
	// Check the runtime to ensure it's close to the timeout, not the server delay
	curTs := time.Now()
	loggedReq := <-recorder
	runTimeSecs := int(curTs.Unix() - loggedReq.ts.Unix())
	assert.GreaterOrEqual(t, runTimeSecs, 2) // Should be at least the timeout duration
	assert.Less(t, runTimeSecs, 8)           // Should be less than the server delay (allowing buffer)

	// Check the DeadlineExceeded error.
	if res.GetStatus().GetCode() != int32(codes.DeadlineExceeded) {
		// Some clients wrap the error code in the message
		msg := res.GetStatus().GetMessage()
		assert.Contains(t, strings.ToLower(strings.ReplaceAll(msg, " ", "")), "deadlineexceeded")
	}
}

// Tests that the PrepareQuery RPC respects the client-specified deadline/timeout
func TestExecuteQuery_PrepareQueryRespectsDeadline(t *testing.T) {
	// 1. Instantiate the mock server with a delay in PrepareQuery longer than the client timeout
	server := initMockServer(t)
	prepareRecorder := make(chan *prepareQueryReqRecord, 1)
	server.PrepareQueryFn = mockPrepareQueryFn(prepareRecorder,
		&prepareQueryAction{
			response: prepareResponse([]byte("foo"), md(
				column("test", strType()),
			)),
			delayStr: "10s", // Server delay
		},
	)
	// ExecuteQueryFn setup is not strictly needed as PrepareQuery should time out first
	// 2. Build the request to test proxy
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	// Set client options with a timeout shorter than the server delay
	opts := &clientOpts{
		timeout: &durationpb.Duration{Seconds: 2}, // Client timeout
	}
	// 3. Perform the operation via test proxy with timeout options
	res := doExecuteQueryOp(t, server, &req, opts)
	// 4. Verify the operation times out during PrepareQuery and returns a DeadlineExceeded error
	// Check the runtime to ensure it's close to the timeout, not the server delay
	curTs := time.Now()
	loggedReq := <-prepareRecorder
	runTimeSecs := int(curTs.Unix() - loggedReq.ts.Unix())
	assert.GreaterOrEqual(t, runTimeSecs, 2)
	assert.Less(t, runTimeSecs, 8)

	if res.GetStatus().GetCode() != int32(codes.DeadlineExceeded) {
		// Some clients wrap the error code in the message
		msg := res.GetStatus().GetMessage()
		assert.Contains(t, strings.ToLower(strings.ReplaceAll(msg, " ", "")), "deadlineexceeded")
	}
}

func TestExecuteQuery_ConcurrentRequests(t *testing.T) {
	concurrency := 5
	server := initMockServer(t)
	prepareRecorder := make(chan *prepareQueryReqRecord, 2)
	prepareQueryMap := map[string]*prepareQueryAction{
		"query0": &prepareQueryAction{
			response: prepareResponse([]byte("query0"), md(
				column("strCol", strType()),
			)),
			delayStr: "2s",
		},
		"query1": &prepareQueryAction{
			response: prepareResponse([]byte("query1"), md(column("intCol", int64Type()), column("boolCol", boolType()))),
			delayStr: "2s",
		},
		"query2": &prepareQueryAction{
			response: prepareResponse([]byte("query2"), md(column("mapCol", mapType(strType(), strType())), column("strCol", strType()))),
			delayStr: "2s",
		},
		"query3": &prepareQueryAction{
			response: prepareResponse([]byte("query3"), md(column("strCol", strType()), column("bytesCol", bytesType()))),
			delayStr: "2s",
		},
		"query4": &prepareQueryAction{
			response: prepareResponse([]byte("query4"), md(column("arrayOfString", arrayType(strType())))),
			delayStr: "2s",
		},
	}
	server.PrepareQueryFn = mockPrepareQueryFnWithMatchingQuery(prepareRecorder, prepareQueryMap)

	recorder := make(chan *executeQueryReqRecord, concurrency)
	actionSequences := map[string][]*executeQueryAction{
		"query0": []*executeQueryAction{
			&executeQueryAction{
				response:    partialResultSet("token", strVal("foo"), strVal("bar"), strVal("baz")),
				endOfStream: true,
			},
		},
		"query1": []*executeQueryAction{
			&executeQueryAction{
				response:    partialResultSet("token", intVal(1), boolVal(true)),
				delayStr:    "2s",
				endOfStream: false,
			},
			&executeQueryAction{
				response:    partialResultSet("token", intVal(2), boolVal(false)),
				delayStr:    "2s",
				endOfStream: true,
			},
		},
		"query2": []*executeQueryAction{
			&executeQueryAction{
				response:    partialResultSet("token", mapVal(mapEntry(strVal("k"), strVal("v"))), strVal("foo")),
				delayStr:    "2s",
				endOfStream: true,
			},
		},
		"query3": []*executeQueryAction{
			&executeQueryAction{
				endOfStream: true,
			},
		},
		"query4": []*executeQueryAction{
			&executeQueryAction{
				response:    partialResultSet("token", arrayVal(strVal("e1"), strVal("e2")), arrayVal(strVal("f1"), strVal("f2"))),
				delayStr:    "2s",
				endOfStream: true,
			},
		},
	}
	server.ExecuteQueryFn = mockExecuteQueryFnWithMetadata(recorder, nil, actionSequences)

	reqs := make([]*testproxypb.ExecuteQueryRequest, concurrency)
	for i := 0; i < concurrency; i++ {
		reqs[i] = &testproxypb.ExecuteQueryRequest{
			ClientId: t.Name(),
			Request: &btpb.ExecuteQueryRequest{
				InstanceName: instanceName,
				Query:        "query" + strconv.Itoa(i),
			},
		}
	}

	results := doExecuteQueryOps(t, server, reqs, nil)

	assert.Equal(t, concurrency, len(recorder))

	// request1
	checkResultOkStatus(t, results[0])
	assert.Equal(t, len(results[0].Metadata.Columns), 1)
	assert.True(t, cmp.Equal(results[0].Metadata, testProxyMd(column("strCol", strType())), protocmp.Transform()))
	assert.Equal(t, len(results[0].Rows), 3)
	assertRowEqual(t, testProxyRow(strVal("foo")), results[0].Rows[0], results[0].Metadata)
	assertRowEqual(t, testProxyRow(strVal("bar")), results[0].Rows[1], results[0].Metadata)
	assertRowEqual(t, testProxyRow(strVal("baz")), results[0].Rows[2], results[0].Metadata)

	// request2
	checkResultOkStatus(t, results[1])
	assert.Equal(t, len(results[1].Metadata.Columns), 2)
	assert.True(t, cmp.Equal(results[1].Metadata, testProxyMd(column("intCol", int64Type()), column("boolCol", boolType())), protocmp.Transform()))
	assert.Equal(t, len(results[1].Rows), 2)
	assertRowEqual(t, testProxyRow(intVal(1), boolVal(true)), results[1].Rows[0], results[1].Metadata)
	assertRowEqual(t, testProxyRow(intVal(2), boolVal(false)), results[1].Rows[1], results[1].Metadata)

	// request3
	checkResultOkStatus(t, results[2])
	assert.Equal(t, len(results[2].Metadata.Columns), 2)
	assert.True(t, cmp.Equal(results[2].Metadata, testProxyMd(column("mapCol", mapType(strType(), strType())), column("strCol", strType())), protocmp.Transform()))
	assert.Equal(t, len(results[2].Rows), 1)
	assertRowEqual(t, testProxyRow(mapVal(mapEntry(strVal("k"), strVal("v"))), strVal("foo")), results[2].Rows[0], results[2].Metadata)

	// request4
	checkResultOkStatus(t, results[3])
	assert.Equal(t, len(results[3].Metadata.Columns), 2)
	assert.True(t, cmp.Equal(results[3].Metadata, testProxyMd(column("strCol", strType()), column("bytesCol", bytesType())), protocmp.Transform()))
	assert.Equal(t, len(results[3].Rows), 0)

	// request5
	checkResultOkStatus(t, results[4])
	assert.Equal(t, len(results[4].Metadata.Columns), 1)
	assert.True(t, cmp.Equal(results[4].Metadata, testProxyMd(column("arrayOfString", arrayType(strType()))), protocmp.Transform()))
	assert.Equal(t, len(results[4].Rows), 2)
	assertRowEqual(t, testProxyRow(arrayVal(strVal("e1"), strVal("e2"))), results[4].Rows[0], results[4].Metadata)
	assertRowEqual(t, testProxyRow(arrayVal(strVal("f1"), strVal("f2"))), results[4].Rows[1], results[4].Metadata)
}

// tests that client doesn't kill inflight requests after client closing, but will reject new requests.
func TestExecuteQuery_CloseClient(t *testing.T) {
	clientID := t.Name()
	server := initMockServer(t)
	prepareRecorder := make(chan *prepareQueryReqRecord, 2)
	// The preparedQuery is used to match to a corresponding executeQueryAction
	// based on the key in the actionSequences map below
	server.PrepareQueryFn = mockPrepareQueryFn(prepareRecorder,
		&prepareQueryAction{
			response: prepareResponse([]byte("query0"), md(
				column("strCol", strType()),
			)),
		},
		&prepareQueryAction{
			response: prepareResponse([]byte("query1"), md(
				column("strCol", strType()),
			)),
		},
		&prepareQueryAction{
			response: prepareResponse([]byte("query2"), md(
				column("strCol", strType()),
			)),
		},
		&prepareQueryAction{
			response: prepareResponse([]byte("query3"), md(
				column("strCol", strType()),
			)),
		},
	)

	executeRecorder := make(chan *executeQueryReqRecord, 4)
	repeatedExecuteAction := []*executeQueryAction{
		&executeQueryAction{
			response:    partialResultSet("token", strVal("foo")),
			delayStr:    "2s",
			endOfStream: true,
		},
	}

	actionSequences := map[string][]*executeQueryAction{
		"query0": repeatedExecuteAction,
		"query1": repeatedExecuteAction,
		"query2": repeatedExecuteAction,
		"query3": repeatedExecuteAction,
	}
	server.ExecuteQueryFn = mockExecuteQueryFnWithMetadata(executeRecorder, nil, actionSequences)

	// Will be finished
	reqsBatchOne := []*testproxypb.ExecuteQueryRequest{
		&testproxypb.ExecuteQueryRequest{
			ClientId: t.Name(),
			Request: &btpb.ExecuteQueryRequest{
				InstanceName: instanceName,
				Query:        "query0",
			},
		},
		&testproxypb.ExecuteQueryRequest{
			ClientId: t.Name(),
			Request: &btpb.ExecuteQueryRequest{
				InstanceName: instanceName,
				Query:        "query1",
			},
		},
	}
	// Will be rejected by client
	reqsBatchTwo := []*testproxypb.ExecuteQueryRequest{
		&testproxypb.ExecuteQueryRequest{
			ClientId: t.Name(),
			Request: &btpb.ExecuteQueryRequest{
				InstanceName: instanceName,
				Query:        "query2",
			},
		},
		&testproxypb.ExecuteQueryRequest{
			ClientId: t.Name(),
			Request: &btpb.ExecuteQueryRequest{
				InstanceName: instanceName,
				Query:        "query3",
			},
		},
	}
	setUp(t, server, clientID, nil)
	defer tearDown(t, server, clientID)

	closeClientAfter := time.Second
	resultsBatchOne := doExecuteQueryOpsCore(t, clientID, reqsBatchOne, &closeClientAfter)
	resultsBatchTwo := doExecuteQueryOpsCore(t, clientID, reqsBatchTwo, nil)

	// Check that server only receives batch-one requests
	assert.Equal(t, 2, len(executeRecorder))

	// Check that all the batch-one requests succeeded or were cancelled
	checkResultOkOrCancelledStatus(t, resultsBatchOne...)
	for i := 0; i < 2; i++ {
		assert.NotNil(t, resultsBatchOne[i])
		if resultsBatchOne[i] == nil {
			continue
		}
		resCode := resultsBatchOne[i].GetStatus().GetCode()
		if resCode == int32(codes.Canceled) {
			continue
		}
		assert.Equal(t, len(resultsBatchOne[i].Metadata.Columns), 1)
		assert.True(t, cmp.Equal(resultsBatchOne[i].Metadata, testProxyMd(column("strCol", strType())), protocmp.Transform()))
		assert.Equal(t, len(resultsBatchOne[i].Rows), 1)
		assertRowEqual(t, testProxyRow(strVal("foo")), resultsBatchOne[i].Rows[0], resultsBatchOne[i].Metadata)
	}

	// Check that all the batch-two requests failed at the proxy level
	assert.NotEmpty(t, resultsBatchTwo[0].GetStatus().GetCode())
	assert.NotEmpty(t, resultsBatchTwo[1].GetStatus().GetCode())
}
