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

def proxy_server_process(queue_map):
    from concurrent import futures

    import grpc
    import test_proxy_pb2
    import test_proxy_pb2_grpc

    # from proxy_server_2 import CreateClient as proxy_client
    def defer_to_client(func, timeout_seconds=10):
        def wrapper(obj, request, context, **kwargs):
            deadline = time.time() + timeout_seconds
            json_dict = json_format.MessageToDict(request)
            json_dict["proxy_request"] = func.__name__
            (in_q, out_q) = queue_map[func.__name__]
            in_q.put(json_dict)
            # wait for response
            while time.time() < deadline:
                if not out_q.empty():
                    response = out_q.get()
                    print(f"read rows response: {response=}")
                    return func(obj, request, context, **kwargs, response=response)
                else:
                    time.sleep(0.1)
        return wrapper


    class TestProxyServer(test_proxy_pb2_grpc.CloudBigtableV2TestProxyServicer):

        def CreateClient(self, request, context):
            return test_proxy_pb2.CreateClientResponse()


        def CloseClient(self, request, context):
            print("close the client")
            print(f"{request=}")
            return test_proxy_pb2.CloseClientResponse()

        def ReadRow(self, request, context):
            print(f"readrow: {request=}")
            return test_proxy_pb2.RowResult()

        @defer_to_client
        def ReadRows(self, request, context, response=None):
            print(f"read rows response: {response=}")
            return test_proxy_pb2.RowsResult()

        def MutateRow(self, request, context):
            print(f"mutate rows: {request.client_id=} {request.request=}" )
            return test_proxy_pb2.MutateRowResult()

        def RemoveClient(self, request, context):
            print(f"removeclient request {request.client_id=}")
            print(request)
            return test_proxy_pb2.RemoveClientResponse()

    port = '50055'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    test_proxy_pb2_grpc.add_CloudBigtableV2TestProxyServicer_to_server(TestProxyServer(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()

def client_process(queue_map):
    import google.cloud.bigtable_v2 as bigtable_v2
    print("listening")
    client = bigtable_v2.BigtableClient()

    class TestProxyHandler():

        def ReadRows(self, request_dict):
            return {"status": "ok"}

    handler = TestProxyHandler()
    in_queues = [queue.in_q for queue in queue_map.values()]
    while True:
        for q in in_queues:
            if not q.empty():
                json_data = q.get()
                # print(json_data)
                fn = getattr(handler, json_data["proxy_request"])
                result = fn(json_data)
                (_, out_q) = queue_map[json_data["proxy_request"]]
                out_q.put(result)
        time.sleep(0.1)

if __name__ == '__main__':
    rpc_names = ["ReadRows"]
    RpcQueues = namedtuple("RpcQueues", ["in_q", "out_q"])
    queue_map = {name: RpcQueues(multiprocessing.Queue(), multiprocessing.Queue()) for name in rpc_names}
    # proxy_server_process(queue_map)
    logging.basicConfig()
    proxy = Process(target=proxy_server_process, args=(queue_map,))
    proxy.start()
    client = Process(target=client_process, args=(queue_map,))
    client.start()
    proxy.join()
    client.join()
