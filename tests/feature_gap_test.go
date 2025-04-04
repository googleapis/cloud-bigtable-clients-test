// Copyright 2023 Google LLC
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
	"encoding/base64"
	"testing"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestFeatureGap tests that all the optional features of Cloud Bigtable clients are truly enabled.
// Note: the test expects that an enabled feature flag to be added to the header of EVERY RPC call,
// even if the feature is not exercised.
func TestFeatureGap(t *testing.T) {
	if !*enableFeaturesAll {
		t.Logf("Skip the check as --enable_features_all is false")
		return
	}

	tableName := buildTableName("table")

	// 1. Instantiate the mock server.
	headers := make(chan metadata.MD, 1)
	server := initMockServer(t)
	server.ReadRowsFn = func(req *btpb.ReadRowsRequest, srv btpb.Bigtable_ReadRowsServer) error {
		md, _ := metadata.FromIncomingContext(srv.Context())
		headers <- md
		return nil
	}

	// 2. Build the request to test proxy
	req := testproxypb.ReadRowRequest{
		ClientId:  t.Name(),
		TableName: tableName,
		RowKey:    "row-01",
	}

	// 3. Perform the operation via test proxy
	doReadRowOp(t, server, &req, nil)

	// 4. Extract, decode, and parse the "bigtable-features" field in the header
	header := <-headers
	if len(header["bigtable-features"]) == 0 {
		t.Fatalf("bigtable-features is missing in the request header")
	}
	featureEncoding := header["bigtable-features"][0]
	t.Logf("Encoded feature flags: %s", featureEncoding)

	decode, err := base64.URLEncoding.DecodeString(featureEncoding)
	if err != nil {
		t.Fatalf("Failed to decode the feature encoding: %v", err)
	}
	t.Logf("Decoded feature flags: %s", decode)

	featureProto := &btpb.FeatureFlags{}
	if err = proto.Unmarshal(decode, featureProto); err != nil {
		t.Fatalf("Failed to parse the decode: %v", err)
	}
	t.Logf("Parsed feature flags: %s", featureProto)

	// 5. Check the featureProto to see if every feature is truly enabled.
	fields := featureProto.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		t.Run(field.TextName(), func(t *testing.T) {
			value := featureProto.ProtoReflect().Get(field)
			assert.True(t, field.Kind() != protoreflect.BoolKind || value.Bool(), "boolean feature flag is not enabled")
		})
	}
}
