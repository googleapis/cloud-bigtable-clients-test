# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import test_proxy_pb2 as test__proxy__pb2


class CloudBigtableV2TestProxyStub(object):
    """Note that all RPCs are unary, even when the equivalent client binding call
    may be streaming. This is an intentional simplification.

    Most methods have sync (default) and async variants. For async variants,
    the proxy is expected to perform the async operation, then wait for results
    before delivering them back to the driver client.

    Operations that may have interesting concurrency characteristics are
    represented explicitly in the API (see ReadRowsRequest.cancel_after_rows).
    We include such operations only when they can be meaningfully performed
    through client bindings.

    Users should generally avoid setting deadlines for requests to the Proxy
    because operations are not cancelable. If the deadline is set anyway, please
    understand that the underlying operation will continue to be executed even
    after the deadline expires.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.CreateClient = channel.unary_unary(
                '/google.bigtable.testproxy.CloudBigtableV2TestProxy/CreateClient',
                request_serializer=test__proxy__pb2.CreateClientRequest.SerializeToString,
                response_deserializer=test__proxy__pb2.CreateClientResponse.FromString,
                )
        self.CloseClient = channel.unary_unary(
                '/google.bigtable.testproxy.CloudBigtableV2TestProxy/CloseClient',
                request_serializer=test__proxy__pb2.CloseClientRequest.SerializeToString,
                response_deserializer=test__proxy__pb2.CloseClientResponse.FromString,
                )
        self.RemoveClient = channel.unary_unary(
                '/google.bigtable.testproxy.CloudBigtableV2TestProxy/RemoveClient',
                request_serializer=test__proxy__pb2.RemoveClientRequest.SerializeToString,
                response_deserializer=test__proxy__pb2.RemoveClientResponse.FromString,
                )
        self.ReadRow = channel.unary_unary(
                '/google.bigtable.testproxy.CloudBigtableV2TestProxy/ReadRow',
                request_serializer=test__proxy__pb2.ReadRowRequest.SerializeToString,
                response_deserializer=test__proxy__pb2.RowResult.FromString,
                )
        self.ReadRows = channel.unary_unary(
                '/google.bigtable.testproxy.CloudBigtableV2TestProxy/ReadRows',
                request_serializer=test__proxy__pb2.ReadRowsRequest.SerializeToString,
                response_deserializer=test__proxy__pb2.RowsResult.FromString,
                )
        self.MutateRow = channel.unary_unary(
                '/google.bigtable.testproxy.CloudBigtableV2TestProxy/MutateRow',
                request_serializer=test__proxy__pb2.MutateRowRequest.SerializeToString,
                response_deserializer=test__proxy__pb2.MutateRowResult.FromString,
                )
        self.BulkMutateRows = channel.unary_unary(
                '/google.bigtable.testproxy.CloudBigtableV2TestProxy/BulkMutateRows',
                request_serializer=test__proxy__pb2.MutateRowsRequest.SerializeToString,
                response_deserializer=test__proxy__pb2.MutateRowsResult.FromString,
                )
        self.CheckAndMutateRow = channel.unary_unary(
                '/google.bigtable.testproxy.CloudBigtableV2TestProxy/CheckAndMutateRow',
                request_serializer=test__proxy__pb2.CheckAndMutateRowRequest.SerializeToString,
                response_deserializer=test__proxy__pb2.CheckAndMutateRowResult.FromString,
                )
        self.SampleRowKeys = channel.unary_unary(
                '/google.bigtable.testproxy.CloudBigtableV2TestProxy/SampleRowKeys',
                request_serializer=test__proxy__pb2.SampleRowKeysRequest.SerializeToString,
                response_deserializer=test__proxy__pb2.SampleRowKeysResult.FromString,
                )
        self.ReadModifyWriteRow = channel.unary_unary(
                '/google.bigtable.testproxy.CloudBigtableV2TestProxy/ReadModifyWriteRow',
                request_serializer=test__proxy__pb2.ReadModifyWriteRowRequest.SerializeToString,
                response_deserializer=test__proxy__pb2.RowResult.FromString,
                )


class CloudBigtableV2TestProxyServicer(object):
    """Note that all RPCs are unary, even when the equivalent client binding call
    may be streaming. This is an intentional simplification.

    Most methods have sync (default) and async variants. For async variants,
    the proxy is expected to perform the async operation, then wait for results
    before delivering them back to the driver client.

    Operations that may have interesting concurrency characteristics are
    represented explicitly in the API (see ReadRowsRequest.cancel_after_rows).
    We include such operations only when they can be meaningfully performed
    through client bindings.

    Users should generally avoid setting deadlines for requests to the Proxy
    because operations are not cancelable. If the deadline is set anyway, please
    understand that the underlying operation will continue to be executed even
    after the deadline expires.
    """

    def CreateClient(self, request, context):
        """Client management:

        Creates a client in the proxy.
        Each client has its own dedicated channel(s), and can be used concurrently
        and independently with other clients.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CloseClient(self, request, context):
        """Closes a client in the proxy, making it not accept new requests.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RemoveClient(self, request, context):
        """Removes a client in the proxy, making it inaccessible. Client closing
        should be done by CloseClient() separately.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ReadRow(self, request, context):
        """Bigtable operations: for each operation, you should use the synchronous or
        asynchronous variant of the client method based on the `use_async_method`
        setting of the client instance. For starters, you can choose to implement
        one variant, and return UNIMPLEMENTED status for the other.

        Reads a row with the client instance.
        The result row may not be present in the response.
        Callers should check for it (e.g. calling has_row() in C++).
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ReadRows(self, request, context):
        """Reads rows with the client instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def MutateRow(self, request, context):
        """Writes a row with the client instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def BulkMutateRows(self, request, context):
        """Writes multiple rows with the client instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CheckAndMutateRow(self, request, context):
        """Performs a check-and-mutate-row operation with the client instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SampleRowKeys(self, request, context):
        """Obtains a row key sampling with the client instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ReadModifyWriteRow(self, request, context):
        """Performs a read-modify-write operation with the client.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_CloudBigtableV2TestProxyServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'CreateClient': grpc.unary_unary_rpc_method_handler(
                    servicer.CreateClient,
                    request_deserializer=test__proxy__pb2.CreateClientRequest.FromString,
                    response_serializer=test__proxy__pb2.CreateClientResponse.SerializeToString,
            ),
            'CloseClient': grpc.unary_unary_rpc_method_handler(
                    servicer.CloseClient,
                    request_deserializer=test__proxy__pb2.CloseClientRequest.FromString,
                    response_serializer=test__proxy__pb2.CloseClientResponse.SerializeToString,
            ),
            'RemoveClient': grpc.unary_unary_rpc_method_handler(
                    servicer.RemoveClient,
                    request_deserializer=test__proxy__pb2.RemoveClientRequest.FromString,
                    response_serializer=test__proxy__pb2.RemoveClientResponse.SerializeToString,
            ),
            'ReadRow': grpc.unary_unary_rpc_method_handler(
                    servicer.ReadRow,
                    request_deserializer=test__proxy__pb2.ReadRowRequest.FromString,
                    response_serializer=test__proxy__pb2.RowResult.SerializeToString,
            ),
            'ReadRows': grpc.unary_unary_rpc_method_handler(
                    servicer.ReadRows,
                    request_deserializer=test__proxy__pb2.ReadRowsRequest.FromString,
                    response_serializer=test__proxy__pb2.RowsResult.SerializeToString,
            ),
            'MutateRow': grpc.unary_unary_rpc_method_handler(
                    servicer.MutateRow,
                    request_deserializer=test__proxy__pb2.MutateRowRequest.FromString,
                    response_serializer=test__proxy__pb2.MutateRowResult.SerializeToString,
            ),
            'BulkMutateRows': grpc.unary_unary_rpc_method_handler(
                    servicer.BulkMutateRows,
                    request_deserializer=test__proxy__pb2.MutateRowsRequest.FromString,
                    response_serializer=test__proxy__pb2.MutateRowsResult.SerializeToString,
            ),
            'CheckAndMutateRow': grpc.unary_unary_rpc_method_handler(
                    servicer.CheckAndMutateRow,
                    request_deserializer=test__proxy__pb2.CheckAndMutateRowRequest.FromString,
                    response_serializer=test__proxy__pb2.CheckAndMutateRowResult.SerializeToString,
            ),
            'SampleRowKeys': grpc.unary_unary_rpc_method_handler(
                    servicer.SampleRowKeys,
                    request_deserializer=test__proxy__pb2.SampleRowKeysRequest.FromString,
                    response_serializer=test__proxy__pb2.SampleRowKeysResult.SerializeToString,
            ),
            'ReadModifyWriteRow': grpc.unary_unary_rpc_method_handler(
                    servicer.ReadModifyWriteRow,
                    request_deserializer=test__proxy__pb2.ReadModifyWriteRowRequest.FromString,
                    response_serializer=test__proxy__pb2.RowResult.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'google.bigtable.testproxy.CloudBigtableV2TestProxy', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class CloudBigtableV2TestProxy(object):
    """Note that all RPCs are unary, even when the equivalent client binding call
    may be streaming. This is an intentional simplification.

    Most methods have sync (default) and async variants. For async variants,
    the proxy is expected to perform the async operation, then wait for results
    before delivering them back to the driver client.

    Operations that may have interesting concurrency characteristics are
    represented explicitly in the API (see ReadRowsRequest.cancel_after_rows).
    We include such operations only when they can be meaningfully performed
    through client bindings.

    Users should generally avoid setting deadlines for requests to the Proxy
    because operations are not cancelable. If the deadline is set anyway, please
    understand that the underlying operation will continue to be executed even
    after the deadline expires.
    """

    @staticmethod
    def CreateClient(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/google.bigtable.testproxy.CloudBigtableV2TestProxy/CreateClient',
            test__proxy__pb2.CreateClientRequest.SerializeToString,
            test__proxy__pb2.CreateClientResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CloseClient(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/google.bigtable.testproxy.CloudBigtableV2TestProxy/CloseClient',
            test__proxy__pb2.CloseClientRequest.SerializeToString,
            test__proxy__pb2.CloseClientResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RemoveClient(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/google.bigtable.testproxy.CloudBigtableV2TestProxy/RemoveClient',
            test__proxy__pb2.RemoveClientRequest.SerializeToString,
            test__proxy__pb2.RemoveClientResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ReadRow(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/google.bigtable.testproxy.CloudBigtableV2TestProxy/ReadRow',
            test__proxy__pb2.ReadRowRequest.SerializeToString,
            test__proxy__pb2.RowResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ReadRows(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/google.bigtable.testproxy.CloudBigtableV2TestProxy/ReadRows',
            test__proxy__pb2.ReadRowsRequest.SerializeToString,
            test__proxy__pb2.RowsResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def MutateRow(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/google.bigtable.testproxy.CloudBigtableV2TestProxy/MutateRow',
            test__proxy__pb2.MutateRowRequest.SerializeToString,
            test__proxy__pb2.MutateRowResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def BulkMutateRows(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/google.bigtable.testproxy.CloudBigtableV2TestProxy/BulkMutateRows',
            test__proxy__pb2.MutateRowsRequest.SerializeToString,
            test__proxy__pb2.MutateRowsResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CheckAndMutateRow(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/google.bigtable.testproxy.CloudBigtableV2TestProxy/CheckAndMutateRow',
            test__proxy__pb2.CheckAndMutateRowRequest.SerializeToString,
            test__proxy__pb2.CheckAndMutateRowResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SampleRowKeys(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/google.bigtable.testproxy.CloudBigtableV2TestProxy/SampleRowKeys',
            test__proxy__pb2.SampleRowKeysRequest.SerializeToString,
            test__proxy__pb2.SampleRowKeysResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ReadModifyWriteRow(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/google.bigtable.testproxy.CloudBigtableV2TestProxy/ReadModifyWriteRow',
            test__proxy__pb2.ReadModifyWriteRowRequest.SerializeToString,
            test__proxy__pb2.RowResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
