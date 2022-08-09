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
	"bytes"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

// dummyMutateRowRequest returns a dummy MutateRowRequest. The number of mutations is customizable
// via `numMutations`. For simplicity, only "SetCell" is used, family names and timestamps are
// hard-coded.
func dummyMutateRowRequest(tableID string, rowKey []byte, numMutations int) *btpb.MutateRowRequest {
	req := &btpb.MutateRowRequest{
		TableName: buildTableName(tableID),
		RowKey: rowKey,
		Mutations: []*btpb.Mutation{},
	}
	for i := 0; i < numMutations; i++ {
		mutation := &btpb.Mutation{
			Mutation: &btpb.Mutation_SetCell_{
				SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "f",
					ColumnQualifier: []byte("col_" + strconv.Itoa(i)),
					TimestampMicros: 1000,
					Value:           []byte("value_" + strconv.Itoa(i)),
				},
			},
		}
		req.Mutations = append(req.Mutations, mutation)
	}
	return req
}

// TestMutateRow_Basic_NonprintableByteKey tests that client can specify non-printable byte strings as row key.
func TestMutateRow_Basic_NonprintableByteKey(t *testing.T) {
	// 1. Instantiate the mock function
	records := make(chan *mutateRowReqRecord, 1)
	action := mutateRowAction{}
	mockFn := mockMutateRowFn(records, action)

	// 2. Build the request to test proxy
	// Set the nonprintable row key according to https://www.utf8-chartable.de/unicode-utf8-table.pl
	nonprintableByteKey, err := hex.DecodeString("13141516")
	if err != nil {
		t.Fatalf("Unable to convert hex to byte: %v", err)
	}
	req := testproxypb.MutateRowRequest{
		ClientId: "TestMutateRow_Basic_NonprintableByteKey",
		Request:  dummyMutateRowRequest("table", nonprintableByteKey, 1),
	}

	// 3. Conduct the test
	res := runMutateRowTest(t, mockFn, &req, nil)

	// 4. Check that the operation succeeded
	loggedReq := <- records
	assert.Equal(t, 0, bytes.Compare(nonprintableByteKey, loggedReq.req.RowKey))
	assert.Empty(t, res.GetStatus().GetCode())
}

// TestMutateRow_Basic_MultipleMutations tests that client can specify multiple mutations for a row.
func TestMutateRow_Basic_MultipleMutations(t *testing.T) {
	// 1. Instantiate the mock function
	records := make(chan *mutateRowReqRecord, 1)
	action := mutateRowAction{}
	mockFn := mockMutateRowFn(records, action)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowRequest{
		ClientId: "TestMutateRow_Basic_NonprintableByteKey",
		Request:  dummyMutateRowRequest("table", []byte("row-01"), 2),
	}

	// 3. Conduct the test
	res := runMutateRowTest(t, mockFn, &req, nil)

	// 4. Check that the operation succeeded
	loggedReq := <- records
	assert.Equal(t, 2, len(loggedReq.req.Mutations))
	assert.Empty(t, res.GetStatus().GetCode())
}
