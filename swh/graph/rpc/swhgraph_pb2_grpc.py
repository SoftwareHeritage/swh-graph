# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from swh.graph.rpc import swhgraph_pb2 as swh_dot_graph_dot_rpc_dot_swhgraph__pb2


class TraversalServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Traverse = channel.unary_stream(
                '/swh.graph.TraversalService/Traverse',
                request_serializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.Node.FromString,
                )
        self.CountNodes = channel.unary_unary(
                '/swh.graph.TraversalService/CountNodes',
                request_serializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CountResponse.FromString,
                )
        self.CountEdges = channel.unary_unary(
                '/swh.graph.TraversalService/CountEdges',
                request_serializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CountResponse.FromString,
                )
        self.Stats = channel.unary_unary(
                '/swh.graph.TraversalService/Stats',
                request_serializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.StatsRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.StatsResponse.FromString,
                )
        self.CheckSwhid = channel.unary_unary(
                '/swh.graph.TraversalService/CheckSwhid',
                request_serializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CheckSwhidRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CheckSwhidResponse.FromString,
                )


class TraversalServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Traverse(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CountNodes(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CountEdges(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Stats(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CheckSwhid(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_TraversalServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Traverse': grpc.unary_stream_rpc_method_handler(
                    servicer.Traverse,
                    request_deserializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.TraversalRequest.FromString,
                    response_serializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.Node.SerializeToString,
            ),
            'CountNodes': grpc.unary_unary_rpc_method_handler(
                    servicer.CountNodes,
                    request_deserializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.TraversalRequest.FromString,
                    response_serializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CountResponse.SerializeToString,
            ),
            'CountEdges': grpc.unary_unary_rpc_method_handler(
                    servicer.CountEdges,
                    request_deserializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.TraversalRequest.FromString,
                    response_serializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CountResponse.SerializeToString,
            ),
            'Stats': grpc.unary_unary_rpc_method_handler(
                    servicer.Stats,
                    request_deserializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.StatsRequest.FromString,
                    response_serializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.StatsResponse.SerializeToString,
            ),
            'CheckSwhid': grpc.unary_unary_rpc_method_handler(
                    servicer.CheckSwhid,
                    request_deserializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CheckSwhidRequest.FromString,
                    response_serializer=swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CheckSwhidResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'swh.graph.TraversalService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class TraversalService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Traverse(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/swh.graph.TraversalService/Traverse',
            swh_dot_graph_dot_rpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
            swh_dot_graph_dot_rpc_dot_swhgraph__pb2.Node.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CountNodes(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/swh.graph.TraversalService/CountNodes',
            swh_dot_graph_dot_rpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
            swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CountResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CountEdges(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/swh.graph.TraversalService/CountEdges',
            swh_dot_graph_dot_rpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
            swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CountResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Stats(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/swh.graph.TraversalService/Stats',
            swh_dot_graph_dot_rpc_dot_swhgraph__pb2.StatsRequest.SerializeToString,
            swh_dot_graph_dot_rpc_dot_swhgraph__pb2.StatsResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CheckSwhid(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/swh.graph.TraversalService/CheckSwhid',
            swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CheckSwhidRequest.SerializeToString,
            swh_dot_graph_dot_rpc_dot_swhgraph__pb2.CheckSwhidResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
