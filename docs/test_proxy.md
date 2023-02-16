# Test Proxy Implementation Guidance

A test proxy is a server that wraps a Cloud Bigtable client library.
Each test proxy is written in the same language as its corresponding client library
(e.g. Java proxy for a Java client library, C++ proxy for a C++ client library).
The proxy should expose the client library's APIs directly through
an RPC interface in the simplest way possible, passing only trivial amounts of data.
The purpose of test proxies is to allow tests to be executed across different client library implementations.

## Steps

First, please familiarize yourself with building a gRPC server in your selected
language ([Guidance](https://grpc.io/docs/languages/)). For examples of
successfully implemented proxies, see the
[Java proxy](https://github.com/googleapis/java-bigtable/tree/main/test-proxy),
[Go proxy](https://github.com/googleapis/google-cloud-go/tree/main/bigtable/internal/testproxy),
[Node.js proxy](https://github.com/googleapis/nodejs-bigtable/tree/main/testproxy),
and the
[C++ proxy](https://github.com/dbolduc/google-cloud-cpp/tree/cbt-test-proxy-dev-flattened/google/cloud/bigtable/cbt_test_proxy).

Second, you need to implement each individual method in the proxy
([Proto definition](https://github.com/googleapis/cndb-client-testing-protos/blob/main/google/bigtable/testproxy/test_proxy.proto),
[additional notes](#additional-notes)):

*   `CreateClient()`, `CloseClient()`, `RemoveClient()`
*   `ReadRow()`, `ReadRows()`
*   `MutateRow()`, `BulkMutateRows()`
*   `CheckAndMutateRow()`
*   `SampleRowKeys()`
*   `ReadModifyWriteRow()`

You can use either sync or async mode of the client library. Note that some
clients may only support one mode. If your client supports both modes, you can
build two separate test proxy binaries, and test both modes. In implementing the
data methods, you may need to convert between the raw proto message and the data
type of your client library (the latter is generally eaiser to work with in
your client language). Such overhead is a downside of using the test proxy.

Third, you should also implement a **`main`** function to bring up the proxy
server and add a command line parameter to allow specifying a valid port number
at runtime. Additional command line parameters may be needed for custom settings
of client authentication. Please check [additional notes](#additional-notes) for
details.

Last, you should place your test proxy in a directory of the GitHub repo of your
client library. The suggested name pattern is \"*test.\*proxy*\".

## Additional Notes

A difficult part of the proxy implementation lies in `CreateClient()`:

*   The test proxy must store multiple Cloud Bigtable client objects in a data
    structure like map/hash/dict where the key is a unique ID that tags the
    client object. To facilitate concurrent access to the data structure,
    locking is needed.
*   When creating a client object, some configurations need to be set up
    properly, including channel credential, call credential, client timeout,
    etc. It's important to understand how your language handles channel
    encryption and call credential in different scenarios (the first scenario
    below is required):
    *   [Required] For hermetic testing (which applies to this repo's tests),
        you don't need to set call credential, and you can use a **plaintext**
        connection to the mock server.
    *   [Optional] For non-hermetic testing (the server is real and remote), you
        need an **encrypted** channel, and you can optionally provide a custom
        call **credential** (e.g. from service account).
        *   To be more sophisticaed, you can allow the specification of custom
            **root certs** and **ssl target** if the default doesn't work for
            you.

There may be confusion about `CloseClient()` and `RemoveClient()`, the key ideas
are:

*   `CloseClient()` makes the client not accept new requests. For inflight
    requests, the desirable result is that they are not cancelled. If your
    client does not support closing client, then you can implement the method as
    no-op; If your client will cancel inflight requests on closing, then please
    document the behavior clearly, and skip the relevant tests in your
    continuous integration.
*   `RemoveClient()` removes the client object from the map/hash/dict, so that
    the proxy user can no longer see the object. `RemoveClient()` should be
    called after `CloseClient()`.

About the `status` field in data operation's response:

*   It should always represents an error returned by the client library. In other
    words, problems in either the proxy logic or test <--> proxy communication
    shouldn't appear in this field.

*   The response of BulkMutateRows also contains per-mutation status. Test proxy
    should disambiguate between RPC failure and individual entry failure, and
    set MutateRowsResult.status according to the overall RPC status. For test cases
    that are focused on per-mutation failures, we will only check the per-mutation
    status (as different clients may exhibit different overall statuses).
