from abc import ABCMeta
from uuid import uuid4

from simple_amqp import AmqpConnection, AmqpMsg, AmqpParameters

from simple_amqp_rpc.consts import (
    REPLY_ID,
    RPC_CALL_TIMEOUT,
    RPC_EXCHANGE,
    RPC_QUEUE,
    RPC_TOPIC
)
from simple_amqp_rpc.data import RpcCall, RpcResp
from simple_amqp_rpc.encoding import (
    decode_rpc_call,
    decode_rpc_resp,
    encode_rpc_call,
    encode_rpc_resp
)

from .client import RpcClient
from .conn import BaseRpc


class BaseAmqpRpc(BaseRpc, metaclass=ABCMeta):
    REPLY_ID = REPLY_ID
    CLIENT_CLS = RpcClient

    def __init__(
            self,
            conn: AmqpConnection = None,
            params: AmqpParameters = None,
            route: str='service.name',
            call_timeout: int=RPC_CALL_TIMEOUT,
    ):
        super().__init__()
        self.route = route
        self._call_timeout = call_timeout
        if conn is not None:
            self.conn = conn
        else:
            self.conn = self._create_conn(params)

        self._rpc_call_channel = None
        self._rpc_resp_channel = None
        self._publish_routes = set()
        self._response_futures = {}
        self._resp_queue = ''

    def _create_conn(self, params: AmqpParameters):
        raise NotImplementedError

    def configure(self):
        self._create_publish()
        self._create_listen()
        self._create_resp()

    def start(self, auto_reconnect: bool=True, wait: bool=True):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def client(self, service: str, route: str) -> RpcClient:
        self._publish_routes.add(route)
        return self.CLIENT_CLS(self, service, route)

    def send_call(self, call: RpcCall, timeout=RPC_CALL_TIMEOUT) -> RpcResp:
        self.log_call_sent(call)
        if timeout is None or timeout == -1:
            timeout = self._call_timeout

        msg = self._encode_call(call)

        reply_id = self._create_reply_id()
        msg = msg.replace(
            exchange=RPC_EXCHANGE.format(route=call.route),
            topic=RPC_TOPIC,
            reply_to=self._resp_queue,
            correlation_id=reply_id,
        )
        return self._send_call_msg(reply_id, timeout, msg)

    def _send_call_msg(
            self,
            reply_id: str,
            timeout: int,
            msg: AmqpMsg,
    ) -> RpcResp:
        raise NotImplementedError

    def _on_call_message(self, msg: AmqpMsg):
        raise NotImplementedError

    def _on_resp_message(self, msg: AmqpMsg):
        raise NotImplementedError

    def _create_reply_id(self) -> str:
        return self.REPLY_ID.format(id=str(uuid4()))

    def _decode_call(self, msg: AmqpMsg) -> RpcCall:
        return decode_rpc_call(msg, self.route)

    def _encode_call(self, call: RpcCall) -> AmqpMsg:
        return encode_rpc_call(call)

    def _decode_resp(self, msg: AmqpMsg) -> RpcResp:
        return decode_rpc_resp(msg)

    def _encode_resp(self, resp: RpcResp) -> AmqpMsg:
        return encode_rpc_resp(resp)

    def _create_publish(self):
        channel = self.conn.channel()
        for route in self._publish_routes:
            exchange = RPC_EXCHANGE.format(route=route)
            channel.exchange(exchange, 'topic', durable=True)

        self._rpc_call_channel = channel

    def _create_listen(self):
        exchange_name = RPC_EXCHANGE.format(route=self.route)
        queue_name = RPC_QUEUE.format(route=self.route)

        channel = self.conn.channel()
        exchange = channel \
            .exchange(exchange_name, 'topic', durable=True)
        channel \
            .queue(queue_name, auto_delete=True) \
            .bind(exchange, RPC_TOPIC) \
            .consume(self._on_call_message)

    def _create_resp(self):
        channel = self.conn.channel()
        queue = channel.queue(auto_delete=True, exclusive=True)
        queue.consume(
            self._on_resp_message,
            auto_ack=True,
            exclusive=True,
        )
        self._resp_channel = channel
        self._resp_queue = queue.name
