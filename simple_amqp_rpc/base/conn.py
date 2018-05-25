from abc import ABCMeta
from typing import Callable, Tuple

from simple_amqp_rpc.consts import METHOD_NOT_FOUND, SERVICE_NOT_FOUND
from simple_amqp_rpc.data import RpcCall, RpcResp
from simple_amqp_rpc.log import setup_logger
from simple_amqp_rpc.service import Service

from .client import RpcClient


class BaseRpc(metaclass=ABCMeta):
    def __init__(self, logger=None):
        self._services = {}
        self._recv_error_handlers = set()
        self.log = logger if logger is not None else setup_logger()

    def method(self, service: str, name: str=None):
        if service not in self._services:
            self._services[service] = {}

        def decorator(func, name=name):
            if name is None:
                name = func.__name__

            self._services[service][name] = func
            return func

        return decorator

    def add_svc(self, svc: Service, servicer) -> 'BaseRpc':
        service_name = svc.name
        methods = svc.get_methods(servicer)

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

    def log_call_recv(self, call: RpcCall):
        self.log.info('call received [{}->{}]'.format(
            call.service,
            call.method,
        ))

    def log_call_sent(self, call: RpcCall):
        self.log.info('sending call [{}:{}]'.format(
            call.service,
            call.method,
        ))

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
            msg = 'Method [{}:{}] not found'.format(
                service,
                method,
            )
            return None, RpcResp(
                status=METHOD_NOT_FOUND,
                body=msg,
            )

        return handler, None
