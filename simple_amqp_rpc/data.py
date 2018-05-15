from typing import Any, List

from dataclasses import dataclass, replace

from .consts import OK


class Data:
    def replace(self, **kwargs):
        return replace(self, **kwargs)


@dataclass(frozen=True)
class RpcCall(Data):
    route: str
    service: str
    method: str
    args: List[Any]


@dataclass(frozen=True)
class RpcResp(Data):
    status: int
    body: Any = None

    @property
    def ok(self):
        return self.status == OK
