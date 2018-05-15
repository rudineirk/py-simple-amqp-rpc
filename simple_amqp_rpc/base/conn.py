from abc import ABCMeta
from typing import Callable, Tuple

from simple_amqp_rpc.consts import METHOD_NOT_FOUND, SERVICE_NOT_FOUND
from simple_amqp_rpc.data import RpcCall, RpcResp

from .client import RpcClient


class BaseRpc(metaclass=ABCMeta):
    def __init__(self):
        self._services = {}
        self._recv_error_handlers = set()

    def method(self, service: str, name: str=None):
        if service not in self._services:
            self._services[service] = {}

        def decorator(func, name=name):
            if name is None:
                name = func.__name__

            self._services[service][name] = func
            return func

        return decorator

    def add_svc(self, service) -> 'BaseRpc':
        service_name = service.svc.name
        methods = service.svc.get_methods(service)

        if service_name not in self._services:
            self._services[service_name] = {}

        for method, handler in methods.items():
            self._services[service_name][method] = handler

        return self

    def client(self, service: str) -> RpcClient:
        raise NotImplementedError

    def send_call(self, call: RpcCall) -> RpcResp:
        raise NotImplementedError

    def recv_call(self, call: RpcCall) -> RpcResp:
        raise NotImplementedError

    def add_recv_call_error_handler(self, handler):
        self._recv_error_handlers.add(handler)

    def _get_method(
            self,
            service: str,
            method: str,
    ) -> Tuple[Callable, RpcResp]:
        try:
            methods = self._services[service]
        except KeyError:
            msg = 'Service [{}] not found'.format(service)
            return None, RpcResp(
                status=SERVICE_NOT_FOUND,
                body=msg,
            )

        try:
            handler = methods[method]
        except KeyError:
            msg = 'Method [{}->{}] not found'.format(
                service,
                method,
            )
            return None, RpcResp(
                status=METHOD_NOT_FOUND,
                body=msg,
            )

        return handler, None
