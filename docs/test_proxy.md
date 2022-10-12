# Test Proxy Implementation Guidance

A test proxy is a server that wraps a Cloud Bigtable client library.
Each test proxy is written in the same language as its corresponding client library
(e.g. Java proxy for a Java client library, C++ proxy for a C++ client library).
The proxy should expose the client library's APIs directly through
an RPC interface in the simplest way possible, passing only trivial amounts of data.
The purpose of test proxies is to allow tests to be executed across different client library implementations.

## Steps

First, please familiarize yourself with building a gRPC server in your selected language
([Guidance](https://grpc.io/docs/languages/)). For examples of successfully implemented proxies,
see the Java proxy (coming soon) and the
[C++ proxy](https://github.com/dbolduc/google-cloud-cpp/tree/cbt-test-proxy-dev/google/cloud/bigtable/cbt_test_proxy).

Second, you need to implement each individual method in the proxy
([Proto definition](https://github.com/googleapis/cloud-bigtable-clients-test/blob/main/testproxypb/v2_test_proxy.proto)):

*   `CreateClient()`, `CloseClient()`, `RemoveClient()` -> Please check
    [additional notes](#notes)
*   `ReadRow()`, `ReadRows()`
*   `MutateRow()`, `BulkMutateRows()`
*   `CheckAndMutateRow()`
*   `SampleRowKeys()`
*   `ReadModifyWriteRow()`

You can use either sync or async mode of the client library. Note that some clients may only support one mode.
If your client supports both modes, you can build two separate test proxy binaries, and test both modes.

Third, you should also implement a **`main`** function to bring up the proxy
server and add a command line parameter to allow specifying a valid port number
at runtime.

Last, you should place your test proxy in a directory of the GitHub repo of your
client library. The suggested name pattern is \"*test.\*proxy*\".

## Additional Notes{#notes}

A difficult part of the proxy implementation lies in `CreateClient()`:

1.  The test proxy must store multiple Cloud Bigtable client objects in a data
    structure like map/hash/dict where the key is a unique ID that tags the
    client object. To facilitate concurrent access to the data structure,
    locking is needed.
1.  When creating a client object, some configurations need to be set up
    properly, including channel credential, call credential, client timeout,
    etc. It's important to understand how your language handles channel vs. call
    credentials.

There may be confusion about `CloseClient()` and `RemoveClient()`, the key ideas
are:

1.  `RemoveClient()` removes the client object from the map/hash/dict, so proxy
    user can no longer see the object.
1.  `CloseClient()` makes the client not accept new requests. For inflight
    requests, the desirable result is that they are not cancelled (Different
    client libraries may have discrepancy here).
1.  `RemoveClient()` is expected to be called after `CloseClient()` to avoid
    resource leak.
