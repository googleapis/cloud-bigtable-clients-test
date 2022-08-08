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
	"testing"

	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

// dummyCheckAndMutateRowRequest returns a dummy CheckAndMutateRowRequest. For simplicity,
// PredicateFilter is not set, and either TrueMutations or FalseMutations is set (not both).
func dummyCheckAndMutateRowRequest(predicateMatched bool, numMutations int) *btpb.CheckAndMutateRowRequest {
	req := &btpb.CheckAndMutateRowRequest{
		TableName:      tableName,
		RowKey:         []byte("row-01"),
		TrueMutations:  []*btpb.Mutation{},
		FalseMutations: []*btpb.Mutation{},
	}

	for i := 0; i < numMutations; i++ {
		mutation := &btpb.Mutation{
			Mutation: &btpb.Mutation_SetCell_{
				SetCell: &btpb.Mutation_SetCell{},
			},
		}
		if predicateMatched {
			req.TrueMutations = append(req.TrueMutations, mutation)
		} else {
			req.FalseMutations = append(req.FalseMutations, mutation)
		}
	}

	return req
}

// TestCheckAndMutateRow_Basic_TrueMutations tests that client can request true mutations.
func TestCheckAndMutateRow_Basic_TrueMutations(t *testing.T) {
	// 0. Common variable
	const predicateMatched bool = true

	// 1. Instantiate the mock function
	records := make(chan *checkAndMutateRowReqRecord, 1)
	action := checkAndMutateRowAction{predicateMatched: predicateMatched}
	mockFn := mockCheckAndMutateRowFn(records, action)

	// 2. Build the request to test proxy
	req := testproxypb.CheckAndMutateRowRequest{
		ClientId: "TestCheckAndMutateRow_Basic_TrueMutations",
		Request:  dummyCheckAndMutateRowRequest(predicateMatched, 2),
	}

	// 3. Conduct the test
	res := runCheckAndMutateRowTest(t, mockFn, &req, nil)

	// 4. Check that the operation succeeded
	loggedReq := <- records
	assert.Equal(t, 2, len(loggedReq.req.TrueMutations))
	assert.Empty(t, loggedReq.req.FalseMutations)
	assert.Empty(t, res.GetStatus().GetCode())
	assert.True(t, res.Result.PredicateMatched)
}

// TestCheckAndMutateRow_Basic_FalseMutations tests that client can request false mutations.
func TestCheckAndMutateRow_Basic_FalseMutations(t *testing.T) {
	// 0. Common variable
	const predicateMatched bool = false

	// 1. Instantiate the mock function
	records := make(chan *checkAndMutateRowReqRecord, 1)
	action := checkAndMutateRowAction{predicateMatched: predicateMatched}
	mockFn := mockCheckAndMutateRowFn(records, action)

	// 2. Build the request to test proxy
	req := testproxypb.CheckAndMutateRowRequest{
		ClientId: "TestCheckAndMutateRow_Basic_FalseMutations",
		Request:  dummyCheckAndMutateRowRequest(predicateMatched, 2),
	}

	// 3. Conduct the test
	res := runCheckAndMutateRowTest(t, mockFn, &req, nil)

	// 4. Check that the operation succeeded
	loggedReq := <- records
	assert.Equal(t, 2, len(loggedReq.req.FalseMutations))
	assert.Empty(t, loggedReq.req.TrueMutations)
	assert.Empty(t, res.GetStatus().GetCode())
	assert.False(t, res.Result.PredicateMatched)
}

