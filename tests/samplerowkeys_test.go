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

// TestSampleRowKeys_Basic_NoEmptyKey tests that client should accept a list with no empty key.
func TestSampleRowKeys_Basic_NoEmptyKey(t *testing.T) {
	// 1. Instantiate the mock function
	stream := []sampleRowKeysAction{
		sampleRowKeysAction{rowKey: []byte("row-31"), offsetBytes: 30},
		sampleRowKeysAction{rowKey: []byte("row-98"), offsetBytes: 65},
	}
	mockFn := mockSampleRowKeysFn(nil, stream...)

	// 2. Build the request to test proxy
	req := testproxypb.SampleRowKeysRequest{
		ClientId: "TestSampleRowKeys_Basic_NoEmptyKey",
		Request: &btpb.SampleRowKeysRequest{
			TableName: tableName,
		},
	}

	// 3. Conduct the test
	res := runSampleRowKeysTest(t, mockFn, &req, nil)

	// 4. Check that the operation succeeded
	assert.Empty(t, res.GetStatus().GetCode())
	assert.Equal(t, 2, len(res.GetSample()))
	assert.Equal(t, "row-31", string(res.GetSample()[0].RowKey))
	assert.Equal(t, "row-98", string(res.GetSample()[1].RowKey))
}
