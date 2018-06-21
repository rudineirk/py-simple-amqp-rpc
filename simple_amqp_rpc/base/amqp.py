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
            logger=None,
    ):
        super().__init__(logger=logger)
        self.route = route
        self._call_timeout = call_timeout
        if conn is not None:
            self.conn = conn
        else:
            self.conn = self._create_conn(params)

        self._call_encoders = {'json': encode_rpc_call}
        self._call_decoders = {'json': decode_rpc_call}
        self._resp_encoders = {'json': encode_rpc_resp}
        self._resp_decoders = {'json': decode_rpc_resp}

        self._default_encoding = 'json'
        self._route_encodings = {}

        self.setup_stage_name = '1:rpc.setup'
        self.setup_stage = None
        self.listen_stage_name = '2:rpc.listen'
        self.listen_stage = None

        self._rpc_call_channel = None
        self._rpc_resp_channel = None
        self._publish_routes = set()
        self._response_futures = {}
        self._resp_queue = ''

    def _create_conn(self, params: AmqpParameters):
        raise NotImplementedError

    def configure(self):
        self._configure_stages()
        self._create_publish()
        self._create_listen()
        self._create_resp()

    def start(self, auto_reconnect: bool=True):
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
        return self._send_call_msg(reply_id, timeout, msg, call.route)

    def set_route_encoding(self, route: str, encoding: str):
        self._route_encodings[route] = encoding

    def set_default_encoding(self, encoding: str):
        self._default_encoding = encoding

    def add_call_encoder(self, name: str, call_encoder):
        self._call_encoders[name] = call_encoder

    def add_call_decoder(self, name: str, call_decoder):
        self._call_decoders[name] = call_decoder

    def add_resp_encoder(self, name: str, resp_encoder):
        self._resp_encoder[name] = resp_encoder

    def add_resp_decoder(self, name: str, resp_decoder):
        self._resp_decoder[name] = resp_decoder

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

    def _get_encoding(self, route: str):
        return self._route_encodings.get(route, self._default_encoding)

    def _decode_call(self, msg: AmqpMsg) -> RpcCall:
        encoding = self._get_encoding(self.route)
        decoder = self._call_decoders[encoding]
        return decoder(msg, self.route)

    def _encode_call(self, call: RpcCall) -> AmqpMsg:
        encoding = self._get_encoding(call.route)
        encoder = self._call_encoders[encoding]
        return encoder(call)

    def _decode_resp(self, msg: AmqpMsg, route: str) -> RpcResp:
        encoding = self._get_encoding(route)
        decoder = self._resp_decoders[encoding]
        return decoder(msg, route)

    def _encode_resp(self, resp: RpcResp) -> AmqpMsg:
        encoding = self._get_encoding(self.route)
        encoder = self._resp_encoders[encoding]
        return encoder(resp)

    def _configure_stages(self):
        self.setup_stage = self.conn.stage(self.setup_stage_name)
        self.listen_stage = self.conn.stage(self.listen_stage_name)

    def _create_publish(self):
        channel = self.conn.channel(stage=self.setup_stage)
        for route in self._publish_routes:
            exchange = RPC_EXCHANGE.format(route=route)
            channel.exchange(
                exchange,
                'topic',
                durable=True,
                stage=self.setup_stage,
            )

        self._rpc_call_channel = channel

    def _create_listen(self):
        exchange_name = RPC_EXCHANGE.format(route=self.route)
        queue_name = RPC_QUEUE.format(route=self.route)

        channel = self.conn.channel(stage=self.setup_stage)
        exchange = channel \
            .exchange(
                exchange_name,
                'topic',
                durable=True,
                stage=self.setup_stage,
            )
        channel \
            .queue(
                queue_name,
                auto_delete=True,
                stage=self.setup_stage,
            ) \
            .bind(
                exchange,
                RPC_TOPIC,
                stage=self.setup_stage,
            ) \
            .consume(
                self._on_call_message,
                stage=self.listen_stage,
            )

    def _create_resp(self):
        channel = self.conn.channel()
        queue = channel.queue(
            auto_delete=True,
            exclusive=True,
            stage=self.setup_stage,
        )
        queue.consume(
            self._on_resp_message,
            auto_ack=True,
            exclusive=True,
            stage=self.setup_stage,
        )
        self._resp_channel = channel
        self._resp_queue = queue.name
