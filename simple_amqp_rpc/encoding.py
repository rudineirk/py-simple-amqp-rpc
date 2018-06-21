import json

from simple_amqp import AmqpMsg

from .consts import RPC_MESSAGE_TTL
from .data import RpcCall, RpcResp

CONTENT_TYPE_MSGPACK = 'application/msgpack'


def encode_rpc_call(call: RpcCall) -> AmqpMsg:
    payload = json.dumps({
        'service': call.service,
        'method': call.method,
        'args': call.args,
    })
    payload = payload.encode('utf8')
    return AmqpMsg(
        payload=payload,
        content_type=CONTENT_TYPE_MSGPACK,
        expiration=RPC_MESSAGE_TTL,
    )


def decode_rpc_call(msg: AmqpMsg, route: str) -> RpcCall:
    payload = json.loads(msg.payload)
    return RpcCall(
        service=payload['service'],
        method=payload['method'],
        args=payload['args'],
        route=route,
    )


def encode_rpc_resp(resp: RpcResp) -> AmqpMsg:
    payload = json.dumps({
        'status': resp.status,
        'body': resp.body,
    })
    payload = payload.encode('utf8')
    return AmqpMsg(
        payload=payload,
        content_type=CONTENT_TYPE_MSGPACK,
    )


def decode_rpc_resp(msg: AmqpMsg, route: str) -> RpcResp:
    payload = json.loads(msg.payload)
    return RpcResp(
        status=payload['status'],
        body=payload['body'],
    )
