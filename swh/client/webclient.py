# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import requests

from typing import Any, Dict, Generator, List, Union
from urllib.parse import urlparse

from swh.model.identifiers import \
    SNAPSHOT, REVISION, RELEASE, DIRECTORY, CONTENT
from swh.model.identifiers import PersistentId as PID
from swh.model.identifiers import parse_persistent_identifier as parse_pid


PIDish = Union[PID, str]


def _get_pid(pidish: PIDish) -> PID:
    """parse string to PID if needed"""
    if isinstance(pidish, str):
        return parse_pid(pidish)
    else:
        return pidish


def typify(json: Any, obj_type: str) -> Any:
    """type json data using pythonic types where appropriate

    e.g., PID instances instead of textual PIDs, datetime.datetime instances
    instead of textual ISO 8601 timestamps, etc.

    """
    # TODO implement this for real
    if obj_type == SNAPSHOT:
        pass
    elif obj_type == REVISION:
        pass
    elif obj_type == RELEASE:
        pass
    elif obj_type == DIRECTORY:
        pass
    elif obj_type == CONTENT:
        pass
    else:
        raise ValueError(f'invalid object type: {obj_type}')

    return json


def jsonify(res: requests.Response, obj_type: str) -> Any:
    """interpret res body as JSON and return it as (typed) Python data

    """
    return typify(res.json(), obj_type=obj_type)


class WebAPIClient:
    """client for the Software Heritage archive Web API, see

    https://archive.softwareheritage.org/api/

    """

    def __init__(self, api_url='https://archive.softwareheritage.org/api/1'):
        """create a client for the Software Heritage Web API

        see: https://archive.softwareheritage.org/api/

        Args:
            api_url: base URL for API calls (default:
                "https://archive.softwareheritage.org/api/1")

        """
        api_url = api_url.rstrip('/')
        u = urlparse(api_url)

        self.api_url = api_url
        self.api_path = u.path

    def _call(self, query: str, http_method: str = 'get',
              **req_args) -> requests.models.Response:
        """dispatcher for archive API invocation

        Args:
            query: API method to be invoked, rooted at api_url
            http_method: HTTP method to be invoked, one of: 'get', 'head'
            req_args: extra keyword arguments for requests.get()/.head()

        Raises:
            requests.HTTPError: if HTTP request fails and http_method is 'get'

        """
        url = '/'.join([self.api_url, query])
        r = None

        if http_method == 'get':
            r = requests.get(url, **req_args)
            r.raise_for_status()
        elif http_method == 'head':
            r = requests.head(url, **req_args)
        else:
            raise ValueError(f'unsupported HTTP method: {http_method}')

        return r

    def content(self, pid: PIDish, **req_args) -> Dict[str, Any]:
        """retrieve information about a content object

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.get()

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        return jsonify(
            self._call(f'content/sha1_git:{_get_pid(pid).object_id}/',
                       **req_args),
            CONTENT)

    def directory(self, pid: PIDish, **req_args) -> List[Dict[str, Any]]:
        """retrieve information about a directory object

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.get()

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        return jsonify(
            self._call(f'directory/{_get_pid(pid).object_id}/', **req_args),
            DIRECTORY)

    def revision(self, pid: PIDish, **req_args) -> Dict[str, Any]:
        """retrieve information about a revision object

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.get()

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        return jsonify(
            self._call(f'revision/{_get_pid(pid).object_id}/', **req_args),
            REVISION)

    def release(self, pid: PIDish, **req_args) -> Dict[str, Any]:
        """retrieve information about a release object

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.get()

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        return jsonify(
            self._call(f'release/{_get_pid(pid).object_id}/', **req_args),
            RELEASE)

    def snapshot(self, pid: PIDish,
                 **req_args) -> Generator[Dict[str, Any], None, None]:
        """retrieve information about a snapshot object

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.get()

        Returns:
            an iterator over partial snapshots, each containing a subset of
            available branches

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        done = False
        r = None
        query = f'snapshot/{_get_pid(pid).object_id}/'

        while not done:
            r = self._call(query, http_method='get', **req_args)
            yield jsonify(r, SNAPSHOT)
            if 'next' in r.links and 'url' in r.links['next']:
                query = r.links['next']['url']
                if query.startswith(self.api_path):
                    # XXX hackish URL cleaning while we wait for swh-web API to
                    # return complete URLs (a-la GitHub/GitLab) in Link headers
                    # instead of absolute paths rooted at https://archive.s.o/
                    query = query[len(self.api_path):].lstrip('/')
            else:
                done = True

    def content_exists(self, pid: PIDish, **req_args) -> bool:
        """check if a content object exists in the archive

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.head()

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        return bool(self._call(f'content/sha1_git:{_get_pid(pid).object_id}/',
                               http_method='head', **req_args))

    def directory_exists(self, pid: PIDish, **req_args) -> bool:
        """check if a directory object exists in the archive

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.head()

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        return bool(self._call(f'directory/{_get_pid(pid).object_id}/',
                               http_method='head', **req_args))

    def revision_exists(self, pid: PIDish, **req_args) -> bool:
        """check if a revision object exists in the archive

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.head()

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        return bool(self._call(f'revision/{_get_pid(pid).object_id}/',
                               http_method='head', **req_args))

    def release_exists(self, pid: PIDish, **req_args) -> bool:
        """check if a release object exists in the archive

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.head()

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        return bool(self._call(f'release/{_get_pid(pid).object_id}/',
                               http_method='head', **req_args))

    def snapshot_exists(self, pid: PIDish, **req_args) -> bool:
        """check if a snapshot object exists in the archive

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.head()

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        return bool(self._call(f'snapshot/{_get_pid(pid).object_id}/',
                               http_method='head', **req_args))

    def content_raw(self, pid: PIDish,
                    **req_args) -> Generator[bytes, None, None]:
        """iterate over the raw content of a content object

        Args:
            pid: object identifier
            req_args: extra keyword arguments for requests.get()

        Raises:
          requests.HTTPError: if HTTP request fails

        """
        r = self._call(f'content/sha1_git:{_get_pid(pid).object_id}/raw/',
                       stream=True, **req_args)
        r.raise_for_status()

        for chunk in r.iter_content(chunk_size=None, decode_unicode=False):
            yield chunk
