from pkgutil import extend_path
from typing import Iterable

__path__: Iterable[str] = extend_path(__path__, __name__)
