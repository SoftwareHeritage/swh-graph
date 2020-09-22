Software Heritage graph FUSE
============================

The Software Heritage :ref:`data model <data-model>` is a direct acyclic graph
with node of different types that correspond to source code artifacts such as
directories, commits, etc. Using this :ref:`FUSE <swh-graph-cli>` module you can
locally mount, and then navigate as a virtual file system, a part of the archive
(a subgraph) rooted at a node of your choice, identified by a :ref:`SWHID
<persistent-identifiers>`.

To retrieve information about the source code artifacts the FUSE module
interacts over the network with the Software Heritage archive via the archive
:ref:`Web API <swh-web-api-urls>`.

File system representation
--------------------------

Graph node types are represented as:

- `cnt`: regular file, contains the blob bytes stored in the object
- `dir`: directory

    - entries are listed based on their real entry names and permissions

- `rev`: directory

    - `message`: commit message (regular file)
    - `author`, `committer`: authorship information (regular files)
    - `author_date`, `committer_date`: timestamps (regular files containing
      textual ISO 8601 date and time timestamps)
    - `root`: source tree at the time of this revision (directory)
    - `type`: type of originating revisions (regular file containing strings
      like: git, tar, dsc, svn, hg, etc.)
    - `metadata.json`: revision metadata (regular file in JSON format)
    - `synthetic`: whether the object has been synthetized by Software Heritage
      or not (regular file, containing either 0 (false) or 1 (true))

- `rel`: directory

    - `name`: release name (regular file)
    - `message`: release message (regular file)
    - `author`: authorship information (regular file)
    - `date`: release timestamp (regular file containing a textual ISO 8601 date
      and time timestamp)
    - `target`: source tree of the target object (directory)
    - `metadata.json`: revision metadata (regular file in JSON format)
    - `synthetic`: whether the object has been synthetized by Software Heritage
      or not (regular file, containing either 0 (false) or 1 (true))

- `snp`: directory

    - branches entries are listed based on their real names (mangled to account
      for UNIX naming conventions, e.g: no `/` character)

Examples
--------

TODO: show the swh/graph/tests/dataset/example graph image and corresponding
mounting points + examples of different level of information (storage, edge
labels, etc.)
