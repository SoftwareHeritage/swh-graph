# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from swh.graph.grpc import swhgraph_pb2 as swh_dot_graph_dot_grpc_dot_swhgraph__pb2


class TraversalServiceStub(object):
    """Graph traversal service 
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetNode = channel.unary_unary(
                '/swh.graph.TraversalService/GetNode',
                request_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.GetNodeRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Node.FromString,
                )
        self.Traverse = channel.unary_stream(
                '/swh.graph.TraversalService/Traverse',
                request_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Node.FromString,
                )
        self.FindPathTo = channel.unary_unary(
                '/swh.graph.TraversalService/FindPathTo',
                request_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.FindPathToRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Path.FromString,
                )
        self.FindPathBetween = channel.unary_unary(
                '/swh.graph.TraversalService/FindPathBetween',
                request_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.FindPathBetweenRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Path.FromString,
                )
        self.CountNodes = channel.unary_unary(
                '/swh.graph.TraversalService/CountNodes',
                request_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.CountResponse.FromString,
                )
        self.CountEdges = channel.unary_unary(
                '/swh.graph.TraversalService/CountEdges',
                request_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.CountResponse.FromString,
                )
        self.Stats = channel.unary_unary(
                '/swh.graph.TraversalService/Stats',
                request_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.StatsRequest.SerializeToString,
                response_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.StatsResponse.FromString,
                )


class TraversalServiceServicer(object):
    """Graph traversal service 
    """

    def GetNode(self, request, context):
        """GetNode returns a single Node and its properties. 
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Traverse(self, request, context):
        """Traverse performs a breadth-first graph traversal from a set of source
        nodes, then streams the nodes it encounters (if they match a given
        return filter), along with their properties.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def FindPathTo(self, request, context):
        """FindPathTo searches for a shortest path between a set of source nodes
        and a node that matches a specific *criteria*.

        It does so by performing a breadth-first search from the source node,
        until any node that matches the given criteria is found, then follows
        back its parents to return a shortest path from the source set to that
        node.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def FindPathBetween(self, request, context):
        """FindPathBetween searches for a shortest path between a set of source
        nodes and a set of destination nodes.

        It does so by performing a *bidirectional breadth-first search*, i.e.,
        two parallel breadth-first searches, one from the source set ("src-BFS")
        and one from the destination set ("dst-BFS"), until both searches find a
        common node that joins their visited sets. This node is called the
        "midpoint node".
        The path returned is the path src -> ... -> midpoint -> ... -> dst,
        which is always a shortest path between src and dst.

        The graph direction of both BFS can be configured separately. By
        default, the dst-BFS will use the graph in the opposite direction than
        the src-BFS (if direction = FORWARD, by default direction_reverse =
        BACKWARD, and vice-versa). The default behavior is thus to search for
        a shortest path between two nodes in a given direction. However, one
        can also specify FORWARD or BACKWARD for *both* the src-BFS and the
        dst-BFS. This will search for a common descendant or a common ancestor
        between the two sets, respectively. These will be the midpoints of the
        returned path.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CountNodes(self, request, context):
        """CountNodes does the same as Traverse, but only returns the number of
        nodes accessed during the traversal. 
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CountEdges(self, request, context):
        """CountEdges does the same as Traverse, but only returns the number of
        edges accessed during the traversal. 
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Stats(self, request, context):
        """Stats returns various statistics on the overall graph. 
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_TraversalServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetNode': grpc.unary_unary_rpc_method_handler(
                    servicer.GetNode,
                    request_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.GetNodeRequest.FromString,
                    response_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Node.SerializeToString,
            ),
            'Traverse': grpc.unary_stream_rpc_method_handler(
                    servicer.Traverse,
                    request_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.TraversalRequest.FromString,
                    response_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Node.SerializeToString,
            ),
            'FindPathTo': grpc.unary_unary_rpc_method_handler(
                    servicer.FindPathTo,
                    request_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.FindPathToRequest.FromString,
                    response_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Path.SerializeToString,
            ),
            'FindPathBetween': grpc.unary_unary_rpc_method_handler(
                    servicer.FindPathBetween,
                    request_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.FindPathBetweenRequest.FromString,
                    response_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Path.SerializeToString,
            ),
            'CountNodes': grpc.unary_unary_rpc_method_handler(
                    servicer.CountNodes,
                    request_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.TraversalRequest.FromString,
                    response_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.CountResponse.SerializeToString,
            ),
            'CountEdges': grpc.unary_unary_rpc_method_handler(
                    servicer.CountEdges,
                    request_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.TraversalRequest.FromString,
                    response_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.CountResponse.SerializeToString,
            ),
            'Stats': grpc.unary_unary_rpc_method_handler(
                    servicer.Stats,
                    request_deserializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.StatsRequest.FromString,
                    response_serializer=swh_dot_graph_dot_grpc_dot_swhgraph__pb2.StatsResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'swh.graph.TraversalService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class TraversalService(object):
    """Graph traversal service 
    """

    @staticmethod
    def GetNode(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/swh.graph.TraversalService/GetNode',
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.GetNodeRequest.SerializeToString,
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Node.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

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
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Node.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def FindPathTo(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/swh.graph.TraversalService/FindPathTo',
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.FindPathToRequest.SerializeToString,
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Path.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def FindPathBetween(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/swh.graph.TraversalService/FindPathBetween',
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.FindPathBetweenRequest.SerializeToString,
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.Path.FromString,
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
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.CountResponse.FromString,
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
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.TraversalRequest.SerializeToString,
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.CountResponse.FromString,
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
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.StatsRequest.SerializeToString,
            swh_dot_graph_dot_grpc_dot_swhgraph__pb2.StatsResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
