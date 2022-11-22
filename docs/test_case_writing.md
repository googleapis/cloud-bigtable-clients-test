# Test case writing tips

This doc provides some tips for writing a new test case.

# Table of contents

<!--ts-->

*   [Find the right test file](#find-the-right-test-file)
*   [Write the test](#write-the-test)
*   [Helpers for server behavior](#helpers-for-server-behavior)
*   [Helpers for test workflow](#helpers-for-test-workflow)

<!--te-->

## Find the right test file

All the test files are in the folder [*tests*](../tests), which are named as
*\*_test.go*. When deciding which test file to edit, you can follow the
following rules:

*   To test a particular method like "ReadRow", the target file is
    *readrow_test.go*. Other methods are "ReadRows", "MutateRow", "MutateRows",
    "ReadModifyWriteRow", "CheckAndMutateRow", and "SampleRowKeys".
*   To write a comprehensive test, and even performance/stress test, the
    recommendation is to use Bigtable emulator, and the target file is
    *emulator_based_test.go*. We don't expect to have many test cases in this
    file.

## Write the test

All of the non-emulator tests share the same
[naming convention](test_case_naming.md) and structure, which is the focus of
this section. A non-emulator test has the following components:

1.  (Optional) **Common variables**: if a hard-coded value is used more than
    once, it's better to create a variable for it (e.g. `rowKey`).
2.  **Instantiate the mock server**: you need to specify how the mock server
    behaves when receiving the request. For example, if you want to test
    "ReadRows", then you need to set `server.ReadRowsFn = <a mock function>`.
    We provide helpers to customize the mock function, please check out the
    section [Helpers for server behavior](#helpers-for-server-behavior).
3.  **Build the request(s) to the test proxy**: you just need to assemble the
    request(s) based on the [proto](../testproxypb/v2_test_proxy.proto).
    Multiple requests are used for concurrency testing.
4.  **Send the request(s) to the test proxy**: this is where the test workflow
    resides. We provide helpers to hide the details such as server
    setup/teardown and client creation/deletion. You just need to use the right
    helper, please check out the section
    [Helpers for test workflow](#helpers-for-test-workflow).
5.  **Validation**: you can check the response from the test proxy, as well as
    the requests received by the mock server.

## Helpers for server behavior

To customize the server behavior, you need to employ the right data types in
*test_datatype.go* and the helpers in *mock_helper.go*. They work together to
produce the desirable behavior. Take "ReadRows" as an example, the relevant data
types are: `readRowsAction`, `readRowsReqRecord`; the relevant helpers are:
`mockReadRowsFnSimple()`, `mockReadRowsFn()`. The other methods are alike.

Sample code for server returning out-of-order rows:

```go
action := &readRowsAction{
        chunks: []chunkData{
                dummyChunkData("row-01", "v1", Commit),
                dummyChunkData("row-07", "v7", Commit),
                dummyChunkData("row-03", "v3", Commit),
        },
}
server := initMockServer(t)
server.ReadRowsFn = mockReadRowsFnSimple(nil, action)
```

Note that the sample code doesn't use all of the data types and helpers. It's up
to your test scenarios and goals.

## Helpers for test workflow

To start the test workflow, you need to employ the helpers in
*test_workflow.go*. For a method, there are three flavors of helper. Take
"ReadRows" as an example, the relevant helpers are: `doReadRowsOp()`,
`doReadRowsOps()`, and `doReadRowsOpsCore()`. You just need one of them.

As the helpers take care of client object creation, custom settings of client
should be done here (e.g. app profile id and timeout settings).

Sample code for setting a client-side timeout:

```go
opts := clientOpts{
        timeout: &durationpb.Duration{Seconds: 2},
}
res := doReadRowsOp(t, server, &req, &opts)
```
