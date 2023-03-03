# Copyright 2015 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the GRPC server."""

import logging
from multiprocessing import Process
import multiprocessing
import time
from google.protobuf import json_format
import inspect
from collections import namedtuple
import random

def grpc_server_process(request_q, queue_pool):
    """
    Defines a process that hosts a grpc server
    proxies requests to a client_handler_process
    """
    from concurrent import futures

    import grpc
    import test_proxy_pb2
    import test_proxy_pb2_grpc

    import data_pb2

    class TestProxyGrpcServer(test_proxy_pb2_grpc.CloudBigtableV2TestProxyServicer):
        """
        Implements a grpc server that proxies conformance test requests to the client library

        Due to issues with using protoc-compiled protos and client-library
        proto-plus objects in the same process, this server defers requests to
        matching methods in  a TestProxyClientHandler instance in a separate 
        process.
        This happens invisbly in the decorator @defer_to_client, with the 
        results attached to each request as a client_response kwarg
        """

        def __init__(self, queue_pool):
            self.open_queues = list(range(len(queue_pool)))
            self.queue_pool = queue_pool

        def defer_to_client(func, timeout_seconds=10):
            """
            Decorator that transparently passes a request to the client
            handler process, and then attaches the resonse to the wrapped call
            """
            def wrapper(self, request, context, **kwargs):
                deadline = time.time() + timeout_seconds
                json_dict = json_format.MessageToDict(request)
                out_idx = self.open_queues.pop()
                json_dict["proxy_request"] = func.__name__
                json_dict["response_queue_idx"] = out_idx
                out_q = queue_pool[out_idx]
                request_q.put(json_dict)
                # wait for response
                while time.time() < deadline:
                    if not out_q.empty():
                        response = out_q.get()
                        self.open_queues.append(out_idx)
                        if isinstance(response, Exception):
                            raise response
                        else:
                            return func(self, request, context, client_response=response, **kwargs)
            return wrapper

        @defer_to_client
        def CreateClient(self, request, context, client_response=None):
            return test_proxy_pb2.CreateClientResponse()

        @defer_to_client
        def CloseClient(self, request, context, client_response=None):
            return test_proxy_pb2.CloseClientResponse()

        @defer_to_client
        def RemoveClient(self, request, context, client_response=None):
            print(request)
            return test_proxy_pb2.RemoveClientResponse()

        @defer_to_client
        def ReadRows(self, request, context, client_response=None):
            print(f"read rows: num chunks: {len(client_response)}" )
            return test_proxy_pb2.RowsResult()

        def ReadRow(self, request, context):
            print(f"readrow: {request=}")
            return test_proxy_pb2.RowResult()

        def MutateRow(self, request, context):
            print(f"mutate rows: {request.client_id=} {request.request=}" )
            return test_proxy_pb2.MutateRowResult()

    # Start gRPC server
    port = '50055'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    test_proxy_pb2_grpc.add_CloudBigtableV2TestProxyServicer_to_server(
        TestProxyGrpcServer(queue_pool), server
    )
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("grpc_server_process started, listening on " + port)
    server.wait_for_termination()

def client_handler_process(request_q, queue_pool):
    """
    Defines a process that recives Bigtable requests from a grpc_server_process,
    and runs the request using a client library instance
    """
    import google.cloud.bigtable_v2 as bigtable_v2
    from google.cloud.bigtable_v2.services.bigtable.transports.grpc import BigtableGrpcTransport
    import grpc
    from google.api_core import client_options as client_options_lib
    import re

    def camel_to_snake(str):
       return re.sub(r'(?<!^)(?=[A-Z])', '_', str).lower()

    class TestProxyClientHandler():
        """
        Implements the same methods as the grpc server, but handles the client
        library side of the request.

        Requests received in TestProxyGrpcServer are converted to a dictionary,
        and supplied to the TestProxyClientHandler methods as kwargs. 
        The client response is then returned back to the TestProxyGrpcServer
        """
        def __init__(self, data_target=None, project_id=None, instance_id=None, app_profile_id=None, per_operation_timeout=None, **kwargs):
            self.closed = False
            transport = BigtableGrpcTransport(
                channel=grpc.insecure_channel(data_target),
            )
            self.client = bigtable_v2.BigtableClient(transport=transport)
            self.project_id = project_id
            self.instance_id = instance_id
            self.app_profile_id = app_profile_id
            self.per_operation_timeout = per_operation_timeout

        def error_safe(func):
            """
            Catch and pass errors back to the grpc_server_process
            Also check if client is closed before processing requests
            """
            def wrapper(self, *args, **kwargs):
                try:
                    if self.closed:
                        raise RuntimeError("client is closed")
                    return func(self, *args, **kwargs)
                except Exception as e:
                    # exceptions should be raised in grpc_server_process
                    return e
            return wrapper

        def close(self):
            self.closed = True

        @error_safe
        def ReadRows(self, request, **kwargs):
            response = list(self.client.read_rows(request))
            serialized_response = [str(r) for r in response]
            return serialized_response

    # Listen to requests from grpc server process
    print("client_handler_process started")
    client_map = {}
    while True:
        if not request_q.empty():
            json_data = request_q.get()
            json_data = {camel_to_snake(k): v for k, v in json_data.items()}
            if "request" in json_data:
                json_data["request"] = {camel_to_snake(k): v for k, v in json_data["request"].items()}
            # print(json_data)
            fn_name = json_data.pop("proxy_request")
            out_q = queue_pool[json_data.pop("response_queue_idx")]
            client_id = json_data.get("clientId", None)
            client = client_map.get(client_id, None)
            # handle special cases for client creation and deletion
            if fn_name == "CreateClient":
                client = TestProxyClientHandler(**json_data)
                client_map[client_id] = client
                out_q.put(True)
            elif client is None:
                raise RuntimeError("client not found")
            elif fn_name == "CloseClient":
                client.close()
                out_q.put(True)
            elif fn_name == "RemoveClient":
                client_map.pop(json_data["client_id"], None)
                out_q.put(True)
            else:
                # run actual rpc against client
                fn = getattr(client, fn_name)
                result = fn(**json_data)
                out_q.put(result)

if __name__ == '__main__':
    # start and run both processes
    response_queue_pool = [multiprocessing.Queue() for _ in range(100)] # larger pools support more concurrent requests
    request_q = multiprocessing.Queue()
    logging.basicConfig()
    proxy = Process(target=grpc_server_process, args=(request_q, response_queue_pool,))
    proxy.start()
    client = Process(target=client_handler_process, args=(request_q, response_queue_pool,))
    client.start()
    proxy.join()
    client.join()
