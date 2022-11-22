# Test case naming conventions

The name of a test case conforms to `Test<method name>_<tag>_<description>`,
except for those in [*emulator_based_test.go*](../tests/emulator_based_test.go).

`<method name>` is one of the following:

* ReadRow
* ReadRows
* MutateRow
* MutateRows
* ReadModifyWriteRow
* CheckAndMutateRow
* SampleRowKeys

`<tag>` is one of the following with or without the suffix “ClientGap”:

* Generic: The features under test are method-agnostic. For example, all requests should have client and resource info in the header.
* Retry: The test exercises the retry behavior of the client when receiving a transient error for a method.
* NoRetry: The test exercises non-retry behavior of the client. For example, the client can succeed or fail as expected when receiving a specific response for a method.

“ClientGap” means the test case cannot pass for all the client libraries.
ClientGap tests let us keep track of the feature gaps between different client
libraries.

`<description>` is a concise description of the test scenario.

Example names:

* `TestReadModifyWriteRow_NoRetry_MultiValues`
* `TestMutateRows_RetryClientGap_ExponentialBackoff`
* `TestReadRows_Generic_Headers`

