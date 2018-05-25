import traceback

from gevent.event import AsyncResult

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp.gevent import GeventAmqpConnection
from simple_amqp_rpc import RpcCall, RpcResp
from simple_amqp_rpc.base import BaseAmqpRpc
from simple_amqp_rpc.consts import (
    CALL_ARGS_MISMATCH,
    CALL_ERROR,
    OK,
    RPC_CALL_TIMEOUT
)


class GeventAmqpRpc(BaseAmqpRpc):
    def __init__(
            self,
            conn: GeventAmqpConnection = None,
            params: AmqpParameters = None,
            route: str='service.name',
            call_timeout: int=RPC_CALL_TIMEOUT,
            logger=None,
    ):
        super().__init__(
            conn=conn,
            params=params,
            route=route,
            call_timeout=call_timeout,
            logger=None,
        )
        self._response_futures = {}

    def start(self, auto_reconnect: bool=True, wait: bool=True):
        self.conn.start(auto_reconnect, wait)

    def stop(self):
        self.conn.stop()

    def _create_conn(self, params: AmqpParameters):
        return GeventAmqpConnection(params)

    def recv_call(self, call: RpcCall) -> RpcResp:
        self.log_call_recv(call)
        method, error = self._get_method(call.service, call.method)
        if error:
            return error

        resp = None
        try:
            resp = method(*call.args)
        except TypeError:
            return RpcResp(
                status=CALL_ARGS_MISMATCH,
                body='Invalid call arguments',
            )
        except Exception as e:
            if not self._recv_error_handlers:
                traceback.print_exc()
            else:
                for handler in self._recv_error_handlers:
                    handler(e)

            return RpcResp(
                status=CALL_ERROR,
            )

        return RpcResp(
            status=OK,
            body=resp,
        )

    def _send_call_msg(
            self,
            reply_id: str,
            timeout: int,
            msg: AmqpMsg,
    ) -> RpcResp:
        self._rpc_call_channel.publish(msg)
        future = AsyncResult()
        self._response_futures[reply_id] = future
        return future.get(timeout=timeout)

    def _on_call_message(self, msg: AmqpMsg) -> bool:
        call = self._decode_call(msg)
        resp = self.recv_call(call)
        resp_msg = self._encode_resp(resp)

        resp_msg = resp_msg.replace(
            topic=msg.reply_to,
            correlation_id=msg.correlation_id,
        )
        self.conn.publish(self._rpc_call_channel, resp_msg)
        return True

    def _on_resp_message(self, msg: AmqpMsg):
        try:
            future = self._response_futures.pop(msg.correlation_id)
        except KeyError:
            return True

        resp = self._decode_resp(msg)
        future.set(resp)
        return True
