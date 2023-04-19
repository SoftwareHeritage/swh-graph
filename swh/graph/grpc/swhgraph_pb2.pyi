"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.field_mask_pb2
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import typing
import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _GraphDirection:
    ValueType = typing.NewType('ValueType', builtins.int)
    V: typing_extensions.TypeAlias = ValueType
class _GraphDirectionEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_GraphDirection.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    FORWARD: _GraphDirection.ValueType  # 0
    """Forward DAG: ori -> snp -> rel -> rev -> dir -> cnt"""

    BACKWARD: _GraphDirection.ValueType  # 1
    """Transposed DAG: cnt -> dir -> rev -> rel -> snp -> ori"""

class GraphDirection(_GraphDirection, metaclass=_GraphDirectionEnumTypeWrapper):
    """Direction of the graph"""
    pass

FORWARD: GraphDirection.ValueType  # 0
"""Forward DAG: ori -> snp -> rel -> rev -> dir -> cnt"""

BACKWARD: GraphDirection.ValueType  # 1
"""Transposed DAG: cnt -> dir -> rev -> rel -> snp -> ori"""

global___GraphDirection = GraphDirection


class GetNodeRequest(google.protobuf.message.Message):
    """Describe a node to return"""
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SWHID_FIELD_NUMBER: builtins.int
    MASK_FIELD_NUMBER: builtins.int
    swhid: typing.Text
    """SWHID of the node to return"""

    @property
    def mask(self) -> google.protobuf.field_mask_pb2.FieldMask:
        """FieldMask of which fields are to be returned (e.g., "swhid,cnt.length").
        By default, all fields are returned.
        """
        pass
    def __init__(self,
        *,
        swhid: typing.Text = ...,
        mask: typing.Optional[google.protobuf.field_mask_pb2.FieldMask] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_mask",b"_mask","mask",b"mask"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_mask",b"_mask","mask",b"mask","swhid",b"swhid"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_mask",b"_mask"]) -> typing.Optional[typing_extensions.Literal["mask"]]: ...
global___GetNodeRequest = GetNodeRequest

class TraversalRequest(google.protobuf.message.Message):
    """TraversalRequest describes how a breadth-first traversal should be
    performed, and what should be returned to the client.
    """
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SRC_FIELD_NUMBER: builtins.int
    DIRECTION_FIELD_NUMBER: builtins.int
    EDGES_FIELD_NUMBER: builtins.int
    MAX_EDGES_FIELD_NUMBER: builtins.int
    MIN_DEPTH_FIELD_NUMBER: builtins.int
    MAX_DEPTH_FIELD_NUMBER: builtins.int
    RETURN_NODES_FIELD_NUMBER: builtins.int
    MASK_FIELD_NUMBER: builtins.int
    MAX_MATCHING_NODES_FIELD_NUMBER: builtins.int
    @property
    def src(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]:
        """Set of source nodes (SWHIDs)"""
        pass
    direction: global___GraphDirection.ValueType
    """Direction of the graph to traverse. Defaults to FORWARD."""

    edges: typing.Text
    """Edge restriction string (e.g. "rev:dir,dir:cnt").
    Defaults to "*" (all).
    """

    max_edges: builtins.int
    """Maximum number of edges accessed in the traversal, after which it stops.
    Defaults to infinite.
    """

    min_depth: builtins.int
    """Do not return nodes with a depth lower than this number.
    By default, all depths are returned.
    """

    max_depth: builtins.int
    """Maximum depth of the traversal, after which it stops.
    Defaults to infinite.
    """

    @property
    def return_nodes(self) -> global___NodeFilter:
        """Filter which nodes will be sent to the stream. By default, all nodes are
        returned.
        """
        pass
    @property
    def mask(self) -> google.protobuf.field_mask_pb2.FieldMask:
        """FieldMask of which fields are to be returned (e.g., "swhid,cnt.length").
        By default, all fields are returned.
        """
        pass
    max_matching_nodes: builtins.int
    """Maximum number of matching results before stopping. For Traverse(), this is
    the total number of results. Defaults to infinite.
    """

    def __init__(self,
        *,
        src: typing.Optional[typing.Iterable[typing.Text]] = ...,
        direction: global___GraphDirection.ValueType = ...,
        edges: typing.Optional[typing.Text] = ...,
        max_edges: typing.Optional[builtins.int] = ...,
        min_depth: typing.Optional[builtins.int] = ...,
        max_depth: typing.Optional[builtins.int] = ...,
        return_nodes: typing.Optional[global___NodeFilter] = ...,
        mask: typing.Optional[google.protobuf.field_mask_pb2.FieldMask] = ...,
        max_matching_nodes: typing.Optional[builtins.int] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_edges",b"_edges","_mask",b"_mask","_max_depth",b"_max_depth","_max_edges",b"_max_edges","_max_matching_nodes",b"_max_matching_nodes","_min_depth",b"_min_depth","_return_nodes",b"_return_nodes","edges",b"edges","mask",b"mask","max_depth",b"max_depth","max_edges",b"max_edges","max_matching_nodes",b"max_matching_nodes","min_depth",b"min_depth","return_nodes",b"return_nodes"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_edges",b"_edges","_mask",b"_mask","_max_depth",b"_max_depth","_max_edges",b"_max_edges","_max_matching_nodes",b"_max_matching_nodes","_min_depth",b"_min_depth","_return_nodes",b"_return_nodes","direction",b"direction","edges",b"edges","mask",b"mask","max_depth",b"max_depth","max_edges",b"max_edges","max_matching_nodes",b"max_matching_nodes","min_depth",b"min_depth","return_nodes",b"return_nodes","src",b"src"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_edges",b"_edges"]) -> typing.Optional[typing_extensions.Literal["edges"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_mask",b"_mask"]) -> typing.Optional[typing_extensions.Literal["mask"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_max_depth",b"_max_depth"]) -> typing.Optional[typing_extensions.Literal["max_depth"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_max_edges",b"_max_edges"]) -> typing.Optional[typing_extensions.Literal["max_edges"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_max_matching_nodes",b"_max_matching_nodes"]) -> typing.Optional[typing_extensions.Literal["max_matching_nodes"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_min_depth",b"_min_depth"]) -> typing.Optional[typing_extensions.Literal["min_depth"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_return_nodes",b"_return_nodes"]) -> typing.Optional[typing_extensions.Literal["return_nodes"]]: ...
global___TraversalRequest = TraversalRequest

class FindPathToRequest(google.protobuf.message.Message):
    """FindPathToRequest describes a request to find a shortest path between a
    set of nodes and a given target criteria, as well as what should be returned
    in the path.
    """
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SRC_FIELD_NUMBER: builtins.int
    TARGET_FIELD_NUMBER: builtins.int
    DIRECTION_FIELD_NUMBER: builtins.int
    EDGES_FIELD_NUMBER: builtins.int
    MAX_EDGES_FIELD_NUMBER: builtins.int
    MAX_DEPTH_FIELD_NUMBER: builtins.int
    MASK_FIELD_NUMBER: builtins.int
    @property
    def src(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]:
        """Set of source nodes (SWHIDs)"""
        pass
    @property
    def target(self) -> global___NodeFilter:
        """Target criteria, i.e., what constitutes a valid path destination."""
        pass
    direction: global___GraphDirection.ValueType
    """Direction of the graph to traverse. Defaults to FORWARD."""

    edges: typing.Text
    """Edge restriction string (e.g. "rev:dir,dir:cnt").
    Defaults to "*" (all).
    """

    max_edges: builtins.int
    """Maximum number of edges accessed in the traversal, after which it stops.
    Defaults to infinite.
    """

    max_depth: builtins.int
    """Maximum depth of the traversal, after which it stops.
    Defaults to infinite.
    """

    @property
    def mask(self) -> google.protobuf.field_mask_pb2.FieldMask:
        """FieldMask of which fields are to be returned (e.g., "swhid,cnt.length").
        By default, all fields are returned.
        """
        pass
    def __init__(self,
        *,
        src: typing.Optional[typing.Iterable[typing.Text]] = ...,
        target: typing.Optional[global___NodeFilter] = ...,
        direction: global___GraphDirection.ValueType = ...,
        edges: typing.Optional[typing.Text] = ...,
        max_edges: typing.Optional[builtins.int] = ...,
        max_depth: typing.Optional[builtins.int] = ...,
        mask: typing.Optional[google.protobuf.field_mask_pb2.FieldMask] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_edges",b"_edges","_mask",b"_mask","_max_depth",b"_max_depth","_max_edges",b"_max_edges","edges",b"edges","mask",b"mask","max_depth",b"max_depth","max_edges",b"max_edges","target",b"target"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_edges",b"_edges","_mask",b"_mask","_max_depth",b"_max_depth","_max_edges",b"_max_edges","direction",b"direction","edges",b"edges","mask",b"mask","max_depth",b"max_depth","max_edges",b"max_edges","src",b"src","target",b"target"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_edges",b"_edges"]) -> typing.Optional[typing_extensions.Literal["edges"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_mask",b"_mask"]) -> typing.Optional[typing_extensions.Literal["mask"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_max_depth",b"_max_depth"]) -> typing.Optional[typing_extensions.Literal["max_depth"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_max_edges",b"_max_edges"]) -> typing.Optional[typing_extensions.Literal["max_edges"]]: ...
global___FindPathToRequest = FindPathToRequest

class FindPathBetweenRequest(google.protobuf.message.Message):
    """FindPathToRequest describes a request to find a shortest path between a
    set of source nodes and a set of destination nodes. It works by performing a
    bidirectional breadth-first traversal from both sets at the same time.
    """
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SRC_FIELD_NUMBER: builtins.int
    DST_FIELD_NUMBER: builtins.int
    DIRECTION_FIELD_NUMBER: builtins.int
    DIRECTION_REVERSE_FIELD_NUMBER: builtins.int
    EDGES_FIELD_NUMBER: builtins.int
    EDGES_REVERSE_FIELD_NUMBER: builtins.int
    MAX_EDGES_FIELD_NUMBER: builtins.int
    MAX_DEPTH_FIELD_NUMBER: builtins.int
    MASK_FIELD_NUMBER: builtins.int
    @property
    def src(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]:
        """Set of source nodes (SWHIDs)"""
        pass
    @property
    def dst(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]:
        """Set of destination nodes (SWHIDs)"""
        pass
    direction: global___GraphDirection.ValueType
    """Direction of the graph to traverse from the source set. Defaults to
    FORWARD.
    """

    direction_reverse: global___GraphDirection.ValueType
    """Direction of the graph to traverse from the destination set. Defaults to
    the opposite of `direction`. If direction and direction_reverse are
    identical, it will find the first common successor of both sets in the
    given direction.
    """

    edges: typing.Text
    """Edge restriction string for the traversal from the source set.
    (e.g. "rev:dir,dir:cnt"). Defaults to "*" (all).
    """

    edges_reverse: typing.Text
    """Edge restriction string for the reverse traversal from the destination
    set.
    If not specified:
      - If `edges` is not specified either, defaults to "*"
      - If direction == direction_reverse, defaults to `edges`
      - If direction != direction_reverse, defaults to the reverse of `edges`
        (e.g. "rev:dir" becomes "dir:rev").
    """

    max_edges: builtins.int
    """Maximum number of edges accessed in the traversal, after which it stops.
    Defaults to infinite.
    """

    max_depth: builtins.int
    """Maximum depth of the traversal, after which it stops.
    Defaults to infinite.
    """

    @property
    def mask(self) -> google.protobuf.field_mask_pb2.FieldMask:
        """FieldMask of which fields are to be returned (e.g., "swhid,cnt.length").
        By default, all fields are returned.
        """
        pass
    def __init__(self,
        *,
        src: typing.Optional[typing.Iterable[typing.Text]] = ...,
        dst: typing.Optional[typing.Iterable[typing.Text]] = ...,
        direction: global___GraphDirection.ValueType = ...,
        direction_reverse: typing.Optional[global___GraphDirection.ValueType] = ...,
        edges: typing.Optional[typing.Text] = ...,
        edges_reverse: typing.Optional[typing.Text] = ...,
        max_edges: typing.Optional[builtins.int] = ...,
        max_depth: typing.Optional[builtins.int] = ...,
        mask: typing.Optional[google.protobuf.field_mask_pb2.FieldMask] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_direction_reverse",b"_direction_reverse","_edges",b"_edges","_edges_reverse",b"_edges_reverse","_mask",b"_mask","_max_depth",b"_max_depth","_max_edges",b"_max_edges","direction_reverse",b"direction_reverse","edges",b"edges","edges_reverse",b"edges_reverse","mask",b"mask","max_depth",b"max_depth","max_edges",b"max_edges"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_direction_reverse",b"_direction_reverse","_edges",b"_edges","_edges_reverse",b"_edges_reverse","_mask",b"_mask","_max_depth",b"_max_depth","_max_edges",b"_max_edges","direction",b"direction","direction_reverse",b"direction_reverse","dst",b"dst","edges",b"edges","edges_reverse",b"edges_reverse","mask",b"mask","max_depth",b"max_depth","max_edges",b"max_edges","src",b"src"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_direction_reverse",b"_direction_reverse"]) -> typing.Optional[typing_extensions.Literal["direction_reverse"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_edges",b"_edges"]) -> typing.Optional[typing_extensions.Literal["edges"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_edges_reverse",b"_edges_reverse"]) -> typing.Optional[typing_extensions.Literal["edges_reverse"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_mask",b"_mask"]) -> typing.Optional[typing_extensions.Literal["mask"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_max_depth",b"_max_depth"]) -> typing.Optional[typing_extensions.Literal["max_depth"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_max_edges",b"_max_edges"]) -> typing.Optional[typing_extensions.Literal["max_edges"]]: ...
global___FindPathBetweenRequest = FindPathBetweenRequest

class NodeFilter(google.protobuf.message.Message):
    """Represents various criteria that make a given node "valid". A node is
    only valid if all the subcriteria present in this message are fulfilled.
    """
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    TYPES_FIELD_NUMBER: builtins.int
    MIN_TRAVERSAL_SUCCESSORS_FIELD_NUMBER: builtins.int
    MAX_TRAVERSAL_SUCCESSORS_FIELD_NUMBER: builtins.int
    types: typing.Text
    """Node restriction string. (e.g. "dir,cnt,rev"). Defaults to "*" (all)."""

    min_traversal_successors: builtins.int
    """Minimum number of successors encountered *during the traversal*.
    Default: no constraint
    """

    max_traversal_successors: builtins.int
    """Maximum number of successors encountered *during the traversal*.
    Default: no constraint
    """

    def __init__(self,
        *,
        types: typing.Optional[typing.Text] = ...,
        min_traversal_successors: typing.Optional[builtins.int] = ...,
        max_traversal_successors: typing.Optional[builtins.int] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_max_traversal_successors",b"_max_traversal_successors","_min_traversal_successors",b"_min_traversal_successors","_types",b"_types","max_traversal_successors",b"max_traversal_successors","min_traversal_successors",b"min_traversal_successors","types",b"types"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_max_traversal_successors",b"_max_traversal_successors","_min_traversal_successors",b"_min_traversal_successors","_types",b"_types","max_traversal_successors",b"max_traversal_successors","min_traversal_successors",b"min_traversal_successors","types",b"types"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_max_traversal_successors",b"_max_traversal_successors"]) -> typing.Optional[typing_extensions.Literal["max_traversal_successors"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_min_traversal_successors",b"_min_traversal_successors"]) -> typing.Optional[typing_extensions.Literal["min_traversal_successors"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_types",b"_types"]) -> typing.Optional[typing_extensions.Literal["types"]]: ...
global___NodeFilter = NodeFilter

class Node(google.protobuf.message.Message):
    """Represents a node in the graph."""
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SWHID_FIELD_NUMBER: builtins.int
    SUCCESSOR_FIELD_NUMBER: builtins.int
    NUM_SUCCESSORS_FIELD_NUMBER: builtins.int
    CNT_FIELD_NUMBER: builtins.int
    REV_FIELD_NUMBER: builtins.int
    REL_FIELD_NUMBER: builtins.int
    ORI_FIELD_NUMBER: builtins.int
    swhid: typing.Text
    """The SWHID of the graph node."""

    @property
    def successor(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Successor]:
        """List of relevant successors of this node."""
        pass
    num_successors: builtins.int
    """Number of relevant successors."""

    @property
    def cnt(self) -> global___ContentData: ...
    @property
    def rev(self) -> global___RevisionData: ...
    @property
    def rel(self) -> global___ReleaseData: ...
    @property
    def ori(self) -> global___OriginData: ...
    def __init__(self,
        *,
        swhid: typing.Text = ...,
        successor: typing.Optional[typing.Iterable[global___Successor]] = ...,
        num_successors: typing.Optional[builtins.int] = ...,
        cnt: typing.Optional[global___ContentData] = ...,
        rev: typing.Optional[global___RevisionData] = ...,
        rel: typing.Optional[global___ReleaseData] = ...,
        ori: typing.Optional[global___OriginData] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_num_successors",b"_num_successors","cnt",b"cnt","data",b"data","num_successors",b"num_successors","ori",b"ori","rel",b"rel","rev",b"rev"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_num_successors",b"_num_successors","cnt",b"cnt","data",b"data","num_successors",b"num_successors","ori",b"ori","rel",b"rel","rev",b"rev","successor",b"successor","swhid",b"swhid"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_num_successors",b"_num_successors"]) -> typing.Optional[typing_extensions.Literal["num_successors"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["data",b"data"]) -> typing.Optional[typing_extensions.Literal["cnt","rev","rel","ori"]]: ...
global___Node = Node

class Path(google.protobuf.message.Message):
    """Represents a path in the graph."""
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    NODE_FIELD_NUMBER: builtins.int
    MIDPOINT_INDEX_FIELD_NUMBER: builtins.int
    @property
    def node(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Node]:
        """List of nodes in the path, from source to destination"""
        pass
    midpoint_index: builtins.int
    """Index of the "midpoint" of the path. For paths obtained with
    bidirectional search queries, this is the node that joined the two
    sets together. When looking for a common ancestor between two nodes by
    performing a FindPathBetween search with two backward graphs, this will
    be the index of the common ancestor in the path.
    """

    def __init__(self,
        *,
        node: typing.Optional[typing.Iterable[global___Node]] = ...,
        midpoint_index: typing.Optional[builtins.int] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_midpoint_index",b"_midpoint_index","midpoint_index",b"midpoint_index"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_midpoint_index",b"_midpoint_index","midpoint_index",b"midpoint_index","node",b"node"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_midpoint_index",b"_midpoint_index"]) -> typing.Optional[typing_extensions.Literal["midpoint_index"]]: ...
global___Path = Path

class Successor(google.protobuf.message.Message):
    """Represents a successor of a given node."""
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SWHID_FIELD_NUMBER: builtins.int
    LABEL_FIELD_NUMBER: builtins.int
    swhid: typing.Text
    """The SWHID of the successor"""

    @property
    def label(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___EdgeLabel]:
        """A list of edge labels for the given edge"""
        pass
    def __init__(self,
        *,
        swhid: typing.Optional[typing.Text] = ...,
        label: typing.Optional[typing.Iterable[global___EdgeLabel]] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_swhid",b"_swhid","swhid",b"swhid"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_swhid",b"_swhid","label",b"label","swhid",b"swhid"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_swhid",b"_swhid"]) -> typing.Optional[typing_extensions.Literal["swhid"]]: ...
global___Successor = Successor

class ContentData(google.protobuf.message.Message):
    """Content node properties"""
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    LENGTH_FIELD_NUMBER: builtins.int
    IS_SKIPPED_FIELD_NUMBER: builtins.int
    length: builtins.int
    """Length of the blob, in bytes"""

    is_skipped: builtins.bool
    """Whether the content was skipped during ingestion."""

    def __init__(self,
        *,
        length: typing.Optional[builtins.int] = ...,
        is_skipped: typing.Optional[builtins.bool] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_is_skipped",b"_is_skipped","_length",b"_length","is_skipped",b"is_skipped","length",b"length"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_is_skipped",b"_is_skipped","_length",b"_length","is_skipped",b"is_skipped","length",b"length"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_is_skipped",b"_is_skipped"]) -> typing.Optional[typing_extensions.Literal["is_skipped"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_length",b"_length"]) -> typing.Optional[typing_extensions.Literal["length"]]: ...
global___ContentData = ContentData

class RevisionData(google.protobuf.message.Message):
    """Revision node properties"""
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    AUTHOR_FIELD_NUMBER: builtins.int
    AUTHOR_DATE_FIELD_NUMBER: builtins.int
    AUTHOR_DATE_OFFSET_FIELD_NUMBER: builtins.int
    COMMITTER_FIELD_NUMBER: builtins.int
    COMMITTER_DATE_FIELD_NUMBER: builtins.int
    COMMITTER_DATE_OFFSET_FIELD_NUMBER: builtins.int
    MESSAGE_FIELD_NUMBER: builtins.int
    author: builtins.int
    """Revision author ID (anonymized)"""

    author_date: builtins.int
    """UNIX timestamp of the revision date (UTC)"""

    author_date_offset: builtins.int
    """Timezone of the revision author date as an offset from UTC"""

    committer: builtins.int
    """Revision committer ID (anonymized)"""

    committer_date: builtins.int
    """UNIX timestamp of the revision committer date (UTC)"""

    committer_date_offset: builtins.int
    """Timezone of the revision committer date as an offset from UTC"""

    message: builtins.bytes
    """Revision message"""

    def __init__(self,
        *,
        author: typing.Optional[builtins.int] = ...,
        author_date: typing.Optional[builtins.int] = ...,
        author_date_offset: typing.Optional[builtins.int] = ...,
        committer: typing.Optional[builtins.int] = ...,
        committer_date: typing.Optional[builtins.int] = ...,
        committer_date_offset: typing.Optional[builtins.int] = ...,
        message: typing.Optional[builtins.bytes] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_author",b"_author","_author_date",b"_author_date","_author_date_offset",b"_author_date_offset","_committer",b"_committer","_committer_date",b"_committer_date","_committer_date_offset",b"_committer_date_offset","_message",b"_message","author",b"author","author_date",b"author_date","author_date_offset",b"author_date_offset","committer",b"committer","committer_date",b"committer_date","committer_date_offset",b"committer_date_offset","message",b"message"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_author",b"_author","_author_date",b"_author_date","_author_date_offset",b"_author_date_offset","_committer",b"_committer","_committer_date",b"_committer_date","_committer_date_offset",b"_committer_date_offset","_message",b"_message","author",b"author","author_date",b"author_date","author_date_offset",b"author_date_offset","committer",b"committer","committer_date",b"committer_date","committer_date_offset",b"committer_date_offset","message",b"message"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_author",b"_author"]) -> typing.Optional[typing_extensions.Literal["author"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_author_date",b"_author_date"]) -> typing.Optional[typing_extensions.Literal["author_date"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_author_date_offset",b"_author_date_offset"]) -> typing.Optional[typing_extensions.Literal["author_date_offset"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_committer",b"_committer"]) -> typing.Optional[typing_extensions.Literal["committer"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_committer_date",b"_committer_date"]) -> typing.Optional[typing_extensions.Literal["committer_date"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_committer_date_offset",b"_committer_date_offset"]) -> typing.Optional[typing_extensions.Literal["committer_date_offset"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_message",b"_message"]) -> typing.Optional[typing_extensions.Literal["message"]]: ...
global___RevisionData = RevisionData

class ReleaseData(google.protobuf.message.Message):
    """Release node properties"""
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    AUTHOR_FIELD_NUMBER: builtins.int
    AUTHOR_DATE_FIELD_NUMBER: builtins.int
    AUTHOR_DATE_OFFSET_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    MESSAGE_FIELD_NUMBER: builtins.int
    author: builtins.int
    """Release author ID (anonymized)"""

    author_date: builtins.int
    """UNIX timestamp of the release date (UTC)"""

    author_date_offset: builtins.int
    """Timezone of the release author date as an offset from UTC"""

    name: builtins.bytes
    """Release name"""

    message: builtins.bytes
    """Release message"""

    def __init__(self,
        *,
        author: typing.Optional[builtins.int] = ...,
        author_date: typing.Optional[builtins.int] = ...,
        author_date_offset: typing.Optional[builtins.int] = ...,
        name: typing.Optional[builtins.bytes] = ...,
        message: typing.Optional[builtins.bytes] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_author",b"_author","_author_date",b"_author_date","_author_date_offset",b"_author_date_offset","_message",b"_message","_name",b"_name","author",b"author","author_date",b"author_date","author_date_offset",b"author_date_offset","message",b"message","name",b"name"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_author",b"_author","_author_date",b"_author_date","_author_date_offset",b"_author_date_offset","_message",b"_message","_name",b"_name","author",b"author","author_date",b"author_date","author_date_offset",b"author_date_offset","message",b"message","name",b"name"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_author",b"_author"]) -> typing.Optional[typing_extensions.Literal["author"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_author_date",b"_author_date"]) -> typing.Optional[typing_extensions.Literal["author_date"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_author_date_offset",b"_author_date_offset"]) -> typing.Optional[typing_extensions.Literal["author_date_offset"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_message",b"_message"]) -> typing.Optional[typing_extensions.Literal["message"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_name",b"_name"]) -> typing.Optional[typing_extensions.Literal["name"]]: ...
global___ReleaseData = ReleaseData

class OriginData(google.protobuf.message.Message):
    """Origin node properties"""
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    URL_FIELD_NUMBER: builtins.int
    url: typing.Text
    """URL of the origin"""

    def __init__(self,
        *,
        url: typing.Optional[typing.Text] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_url",b"_url","url",b"url"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_url",b"_url","url",b"url"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_url",b"_url"]) -> typing.Optional[typing_extensions.Literal["url"]]: ...
global___OriginData = OriginData

class EdgeLabel(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    NAME_FIELD_NUMBER: builtins.int
    PERMISSION_FIELD_NUMBER: builtins.int
    name: builtins.bytes
    """Directory entry name for directories, branch name for snapshots"""

    permission: builtins.int
    """Entry permission (only set for directories)."""

    def __init__(self,
        *,
        name: builtins.bytes = ...,
        permission: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["name",b"name","permission",b"permission"]) -> None: ...
global___EdgeLabel = EdgeLabel

class CountResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    COUNT_FIELD_NUMBER: builtins.int
    count: builtins.int
    def __init__(self,
        *,
        count: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["count",b"count"]) -> None: ...
global___CountResponse = CountResponse

class StatsRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___StatsRequest = StatsRequest

class StatsResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    NUM_NODES_FIELD_NUMBER: builtins.int
    NUM_EDGES_FIELD_NUMBER: builtins.int
    COMPRESSION_RATIO_FIELD_NUMBER: builtins.int
    BITS_PER_NODE_FIELD_NUMBER: builtins.int
    BITS_PER_EDGE_FIELD_NUMBER: builtins.int
    AVG_LOCALITY_FIELD_NUMBER: builtins.int
    INDEGREE_MIN_FIELD_NUMBER: builtins.int
    INDEGREE_MAX_FIELD_NUMBER: builtins.int
    INDEGREE_AVG_FIELD_NUMBER: builtins.int
    OUTDEGREE_MIN_FIELD_NUMBER: builtins.int
    OUTDEGREE_MAX_FIELD_NUMBER: builtins.int
    OUTDEGREE_AVG_FIELD_NUMBER: builtins.int
    EXPORT_STARTED_AT_FIELD_NUMBER: builtins.int
    EXPORT_ENDED_AT_FIELD_NUMBER: builtins.int
    num_nodes: builtins.int
    """Number of nodes in the graph"""

    num_edges: builtins.int
    """Number of edges in the graph"""

    compression_ratio: builtins.float
    """Ratio between the graph size and the information-theoretical lower
    bound
    """

    bits_per_node: builtins.float
    """Number of bits per node (overall graph size in bits divided by the
    number of nodes)
    """

    bits_per_edge: builtins.float
    """Number of bits per edge (overall graph size in bits divided by the
    number of arcs).
    """

    avg_locality: builtins.float
    indegree_min: builtins.int
    """Smallest indegree"""

    indegree_max: builtins.int
    """Largest indegree"""

    indegree_avg: builtins.float
    """Average indegree"""

    outdegree_min: builtins.int
    """Smallest outdegree"""

    outdegree_max: builtins.int
    """Largest outdegree"""

    outdegree_avg: builtins.float
    """Average outdegree"""

    export_started_at: builtins.int
    """Time when the export started"""

    export_ended_at: builtins.int
    """Time when the export ended"""

    def __init__(self,
        *,
        num_nodes: builtins.int = ...,
        num_edges: builtins.int = ...,
        compression_ratio: builtins.float = ...,
        bits_per_node: builtins.float = ...,
        bits_per_edge: builtins.float = ...,
        avg_locality: builtins.float = ...,
        indegree_min: builtins.int = ...,
        indegree_max: builtins.int = ...,
        indegree_avg: builtins.float = ...,
        outdegree_min: builtins.int = ...,
        outdegree_max: builtins.int = ...,
        outdegree_avg: builtins.float = ...,
        export_started_at: typing.Optional[builtins.int] = ...,
        export_ended_at: typing.Optional[builtins.int] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_export_ended_at",b"_export_ended_at","_export_started_at",b"_export_started_at","export_ended_at",b"export_ended_at","export_started_at",b"export_started_at"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_export_ended_at",b"_export_ended_at","_export_started_at",b"_export_started_at","avg_locality",b"avg_locality","bits_per_edge",b"bits_per_edge","bits_per_node",b"bits_per_node","compression_ratio",b"compression_ratio","export_ended_at",b"export_ended_at","export_started_at",b"export_started_at","indegree_avg",b"indegree_avg","indegree_max",b"indegree_max","indegree_min",b"indegree_min","num_edges",b"num_edges","num_nodes",b"num_nodes","outdegree_avg",b"outdegree_avg","outdegree_max",b"outdegree_max","outdegree_min",b"outdegree_min"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_export_ended_at",b"_export_ended_at"]) -> typing.Optional[typing_extensions.Literal["export_ended_at"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_export_started_at",b"_export_started_at"]) -> typing.Optional[typing_extensions.Literal["export_started_at"]]: ...
global___StatsResponse = StatsResponse
