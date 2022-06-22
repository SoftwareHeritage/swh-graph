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
    BACKWARD: _GraphDirection.ValueType  # 1
    BOTH: _GraphDirection.ValueType  # 2
class GraphDirection(_GraphDirection, metaclass=_GraphDirectionEnumTypeWrapper):
    pass

FORWARD: GraphDirection.ValueType  # 0
BACKWARD: GraphDirection.ValueType  # 1
BOTH: GraphDirection.ValueType  # 2
global___GraphDirection = GraphDirection


class TraversalRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SRC_FIELD_NUMBER: builtins.int
    DIRECTION_FIELD_NUMBER: builtins.int
    EDGES_FIELD_NUMBER: builtins.int
    MAX_EDGES_FIELD_NUMBER: builtins.int
    MIN_DEPTH_FIELD_NUMBER: builtins.int
    MAX_DEPTH_FIELD_NUMBER: builtins.int
    RETURN_NODES_FIELD_NUMBER: builtins.int
    MASK_FIELD_NUMBER: builtins.int
    @property
    def src(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    direction: global___GraphDirection.ValueType
    """Traversal options"""

    edges: typing.Text
    max_edges: builtins.int
    min_depth: builtins.int
    max_depth: builtins.int
    @property
    def return_nodes(self) -> global___NodeFilter: ...
    @property
    def mask(self) -> google.protobuf.field_mask_pb2.FieldMask: ...
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
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_edges",b"_edges","_mask",b"_mask","_max_depth",b"_max_depth","_max_edges",b"_max_edges","_min_depth",b"_min_depth","_return_nodes",b"_return_nodes","edges",b"edges","mask",b"mask","max_depth",b"max_depth","max_edges",b"max_edges","min_depth",b"min_depth","return_nodes",b"return_nodes"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_edges",b"_edges","_mask",b"_mask","_max_depth",b"_max_depth","_max_edges",b"_max_edges","_min_depth",b"_min_depth","_return_nodes",b"_return_nodes","direction",b"direction","edges",b"edges","mask",b"mask","max_depth",b"max_depth","max_edges",b"max_edges","min_depth",b"min_depth","return_nodes",b"return_nodes","src",b"src"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_edges",b"_edges"]) -> typing.Optional[typing_extensions.Literal["edges"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_mask",b"_mask"]) -> typing.Optional[typing_extensions.Literal["mask"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_max_depth",b"_max_depth"]) -> typing.Optional[typing_extensions.Literal["max_depth"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_max_edges",b"_max_edges"]) -> typing.Optional[typing_extensions.Literal["max_edges"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_min_depth",b"_min_depth"]) -> typing.Optional[typing_extensions.Literal["min_depth"]]: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_return_nodes",b"_return_nodes"]) -> typing.Optional[typing_extensions.Literal["return_nodes"]]: ...
global___TraversalRequest = TraversalRequest

class NodeFilter(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    TYPES_FIELD_NUMBER: builtins.int
    MIN_TRAVERSAL_SUCCESSORS_FIELD_NUMBER: builtins.int
    MAX_TRAVERSAL_SUCCESSORS_FIELD_NUMBER: builtins.int
    types: typing.Text
    min_traversal_successors: builtins.int
    max_traversal_successors: builtins.int
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
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SWHID_FIELD_NUMBER: builtins.int
    SUCCESSOR_FIELD_NUMBER: builtins.int
    CNT_FIELD_NUMBER: builtins.int
    REV_FIELD_NUMBER: builtins.int
    REL_FIELD_NUMBER: builtins.int
    ORI_FIELD_NUMBER: builtins.int
    swhid: typing.Text
    @property
    def successor(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Successor]: ...
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
        cnt: typing.Optional[global___ContentData] = ...,
        rev: typing.Optional[global___RevisionData] = ...,
        rel: typing.Optional[global___ReleaseData] = ...,
        ori: typing.Optional[global___OriginData] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["cnt",b"cnt","data",b"data","ori",b"ori","rel",b"rel","rev",b"rev"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["cnt",b"cnt","data",b"data","ori",b"ori","rel",b"rel","rev",b"rev","successor",b"successor","swhid",b"swhid"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["data",b"data"]) -> typing.Optional[typing_extensions.Literal["cnt","rev","rel","ori"]]: ...
global___Node = Node

class Successor(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SWHID_FIELD_NUMBER: builtins.int
    LABEL_FIELD_NUMBER: builtins.int
    swhid: typing.Text
    @property
    def label(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___EdgeLabel]: ...
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
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    LENGTH_FIELD_NUMBER: builtins.int
    IS_SKIPPED_FIELD_NUMBER: builtins.int
    length: builtins.int
    is_skipped: builtins.bool
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
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    AUTHOR_FIELD_NUMBER: builtins.int
    AUTHOR_DATE_FIELD_NUMBER: builtins.int
    AUTHOR_DATE_OFFSET_FIELD_NUMBER: builtins.int
    COMMITTER_FIELD_NUMBER: builtins.int
    COMMITTER_DATE_FIELD_NUMBER: builtins.int
    COMMITTER_DATE_OFFSET_FIELD_NUMBER: builtins.int
    MESSAGE_FIELD_NUMBER: builtins.int
    author: builtins.int
    author_date: builtins.int
    author_date_offset: builtins.int
    committer: builtins.int
    committer_date: builtins.int
    committer_date_offset: builtins.int
    message: builtins.bytes
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
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    AUTHOR_FIELD_NUMBER: builtins.int
    AUTHOR_DATE_FIELD_NUMBER: builtins.int
    AUTHOR_DATE_OFFSET_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    MESSAGE_FIELD_NUMBER: builtins.int
    author: builtins.int
    author_date: builtins.int
    author_date_offset: builtins.int
    name: builtins.bytes
    message: builtins.bytes
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
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    URL_FIELD_NUMBER: builtins.int
    url: typing.Text
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
    permission: builtins.int
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
    COMPRESSION_FIELD_NUMBER: builtins.int
    BITS_PER_NODE_FIELD_NUMBER: builtins.int
    BITS_PER_EDGE_FIELD_NUMBER: builtins.int
    AVG_LOCALITY_FIELD_NUMBER: builtins.int
    INDEGREE_MIN_FIELD_NUMBER: builtins.int
    INDEGREE_MAX_FIELD_NUMBER: builtins.int
    INDEGREE_AVG_FIELD_NUMBER: builtins.int
    OUTDEGREE_MIN_FIELD_NUMBER: builtins.int
    OUTDEGREE_MAX_FIELD_NUMBER: builtins.int
    OUTDEGREE_AVG_FIELD_NUMBER: builtins.int
    num_nodes: builtins.int
    num_edges: builtins.int
    compression: builtins.float
    bits_per_node: builtins.float
    bits_per_edge: builtins.float
    avg_locality: builtins.float
    indegree_min: builtins.int
    indegree_max: builtins.int
    indegree_avg: builtins.float
    outdegree_min: builtins.int
    outdegree_max: builtins.int
    outdegree_avg: builtins.float
    def __init__(self,
        *,
        num_nodes: builtins.int = ...,
        num_edges: builtins.int = ...,
        compression: builtins.float = ...,
        bits_per_node: builtins.float = ...,
        bits_per_edge: builtins.float = ...,
        avg_locality: builtins.float = ...,
        indegree_min: builtins.int = ...,
        indegree_max: builtins.int = ...,
        indegree_avg: builtins.float = ...,
        outdegree_min: builtins.int = ...,
        outdegree_max: builtins.int = ...,
        outdegree_avg: builtins.float = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["avg_locality",b"avg_locality","bits_per_edge",b"bits_per_edge","bits_per_node",b"bits_per_node","compression",b"compression","indegree_avg",b"indegree_avg","indegree_max",b"indegree_max","indegree_min",b"indegree_min","num_edges",b"num_edges","num_nodes",b"num_nodes","outdegree_avg",b"outdegree_avg","outdegree_max",b"outdegree_max","outdegree_min",b"outdegree_min"]) -> None: ...
global___StatsResponse = StatsResponse

class CheckSwhidRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SWHID_FIELD_NUMBER: builtins.int
    swhid: typing.Text
    def __init__(self,
        *,
        swhid: typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["swhid",b"swhid"]) -> None: ...
global___CheckSwhidRequest = CheckSwhidRequest

class GetNodeRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SWHID_FIELD_NUMBER: builtins.int
    MASK_FIELD_NUMBER: builtins.int
    swhid: typing.Text
    @property
    def mask(self) -> google.protobuf.field_mask_pb2.FieldMask: ...
    def __init__(self,
        *,
        swhid: typing.Text = ...,
        mask: typing.Optional[google.protobuf.field_mask_pb2.FieldMask] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_mask",b"_mask","mask",b"mask"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_mask",b"_mask","mask",b"mask","swhid",b"swhid"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_mask",b"_mask"]) -> typing.Optional[typing_extensions.Literal["mask"]]: ...
global___GetNodeRequest = GetNodeRequest

class CheckSwhidResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    EXISTS_FIELD_NUMBER: builtins.int
    DETAILS_FIELD_NUMBER: builtins.int
    exists: builtins.bool
    details: typing.Text
    def __init__(self,
        *,
        exists: builtins.bool = ...,
        details: typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["details",b"details","exists",b"exists"]) -> None: ...
global___CheckSwhidResponse = CheckSwhidResponse