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

def proxy_server_process(request_q, queue_pool):
    from concurrent import futures

    import grpc
    import test_proxy_pb2
    import test_proxy_pb2_grpc




    class TestProxyServer(test_proxy_pb2_grpc.CloudBigtableV2TestProxyServicer):

        def __init__(self, queue_pool):
            self.open_queues = list(range(len(queue_pool)))
            self.queue_pool = queue_pool

        def defer_to_client(func, timeout_seconds=10):
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
                        print(f"{func.__name__} client response: {response=}")
                        return func(self, request, context, **kwargs, response=response)
                    else:
                        time.sleep(0.1)
            return wrapper

        @defer_to_client
        def CreateClient(self, request, context, response=None):
            return test_proxy_pb2.CreateClientResponse()

        @defer_to_client
        def CloseClient(self, request, context):
            return test_proxy_pb2.CloseClientResponse()

        @defer_to_client
        def RemoveClient(self, request, context, response=None):
            print(request)
            return test_proxy_pb2.RemoveClientResponse()

        @defer_to_client
        def ReadRows(self, request, context, response=None):
            return test_proxy_pb2.RowsResult()

        def ReadRow(self, request, context):
            print(f"readrow: {request=}")
            return test_proxy_pb2.RowResult()

        def MutateRow(self, request, context):
            print(f"mutate rows: {request.client_id=} {request.request=}" )
            return test_proxy_pb2.MutateRowResult()



    port = '50055'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    test_proxy_pb2_grpc.add_CloudBigtableV2TestProxyServicer_to_server(TestProxyServer(queue_pool), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()

def client_process(request_q, queue_pool):
    import google.cloud.bigtable_v2 as bigtable_v2
    print("listening")

    class TestProxyHandler():
        def __init__(self):
            self.closed = False
            self.client = bigtable_v2.BigtableClient()

        def close(self):
            self.closed = True

        def ReadRows(self, request_dict):
            if self.closed:
                raise RuntimeError("client is closed")
            return {"status": "ok"}

    client_map = {}
    while True:
        if not request_q.empty():
            json_data = request_q.get()
            # print(json_data)
            fn_name = json_data.pop("proxy_request")
            out_q = queue_pool[json_data.pop("response_queue_idx")]
            client_id = json_data.get("clientId", None)
            client = client_map.get(client_id, None)
            if fn_name == "CreateClient":
                client = TestProxyHandler()
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
                result = fn(json_data)
                out_q.put(result)
        time.sleep(0.1)

if __name__ == '__main__':
    queue_pool = [multiprocessing.Queue() for _ in range(100)]
    request_q = multiprocessing.Queue()
    # proxy_server_process(queue_map)
    logging.basicConfig()
    proxy = Process(target=proxy_server_process, args=(request_q, queue_pool,))
    proxy.start()
    client = Process(target=client_process, args=(request_q, queue_pool,))
    client.start()
    proxy.join()
    client.join()
