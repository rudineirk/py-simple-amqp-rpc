import traceback
from asyncio import Future, wait_for

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp.asyncio import AsyncioAmqpConnection
from simple_amqp_rpc import RpcCall, RpcResp
from simple_amqp_rpc.base import BaseAmqpRpc
from simple_amqp_rpc.consts import (
    CALL_ARGS_MISMATCH,
    CALL_ERROR,
    OK,
    RPC_CALL_TIMEOUT
)


class AsyncioAmqpRpc(BaseAmqpRpc):
    def __init__(
            self,
            conn: AsyncioAmqpConnection = None,
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
            logger=logger,
        )
        self._response_futures = {}

    async def start(self, auto_reconnect: bool=True, wait: bool=True):
        await self.conn.start(auto_reconnect, wait)

    async def stop(self):
        await self.conn.stop()

    def _create_conn(self, params: AmqpParameters):
        return AsyncioAmqpConnection(params)

    async def recv_call(self, call: RpcCall) -> RpcResp:
        self.log_call_recv(call)
        method, error = self._get_method(call.service, call.method)
        if error:
            return error

        resp = None
        try:
            resp = await method(*call.args)
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

    async def _send_call_msg(
            self,
            reply_id: str,
            timeout: int,
            msg: AmqpMsg,
    ) -> RpcResp:
        await self._rpc_call_channel.publish(msg)
        future = Future()
        self._response_futures[reply_id] = future
        return await wait_for(future, timeout)

    async def _on_call_message(self, msg: AmqpMsg) -> bool:
        call = self._decode_call(msg)
        resp = await self.recv_call(call)
        resp_msg = self._encode_resp(resp)

        resp_msg = resp_msg.replace(
            topic=msg.reply_to,
            correlation_id=msg.correlation_id,
        )
        await self.conn.publish(self._rpc_call_channel, resp_msg)
        return True

    async def _on_resp_message(self, msg: AmqpMsg):
        try:
            future = self._response_futures.pop(msg.correlation_id)
        except KeyError:
            return True

        resp = self._decode_resp(msg)
        future.set_result(resp)
        return True
