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

// Package tests implements the test suite for Cloud Bigtable clients.
package tests

import (
	"flag"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"google.golang.org/grpc"
)

var proxyAddr = flag.String("proxy_addr", "",
	"The address of the test proxy server, which exports the CloudBigtableV2TestProxy "+
	"service and should be running already. host:port address is expected. "+
	":port also works as the proxy server is local.")
var printClientReq = flag.Bool("print_client_req", false,
	"If enabled, server will print its received requests from the client. It helps debugging, "+
	"but is quite verbose. Default to false.")
var enableFeaturesAll = flag.Bool("enable_features_all", false,
	"If enabled, client will enable all the optional features before sending out requests.")

// testProxyClient is the stub used by all the test cases to interact with the test proxy.
var testProxyClient testproxypb.CloudBigtableV2TestProxyClient

// TestMain is the entry point of test, where individual test cases are invoked.
func TestMain(m *testing.M) {
	// Parse and validate flags
	flag.Parse()
	if *proxyAddr == "" {
		log.Fatal("Failed to set -proxy_addr, exiting now")
	}

	// Wait for the test proxy server if it's starting
	retry := 0
	for retry < 100 {
		conn, err := net.Dial("tcp", *proxyAddr)
		if err == nil && conn != nil {
			conn.Close()
			break
		}
		log.Println("Test Proxy is not ready, waiting...")
		time.Sleep(time.Second)
		retry++
	}
	if retry >= 100 {
		log.Fatal("Test Proxy is not available, exiting now")
	}

	// Create a test proxy client
	conn, _ := grpc.Dial(*proxyAddr, grpc.WithInsecure())
	defer conn.Close()
	testProxyClient = testproxypb.NewCloudBigtableV2TestProxyClient(conn)

	// Invoke the test cases
	exitVal := m.Run()
	os.Exit(exitVal)
}
