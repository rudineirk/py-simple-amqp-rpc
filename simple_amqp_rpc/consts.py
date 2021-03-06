from http import HTTPStatus

RPC_EXCHANGE = 'rpc.{route}'
RPC_QUEUE = 'rpc.{route}'
REPLY_ID = 'rpc.reply.{id}'
RPC_TOPIC = 'rpc'
RPC_CALL_TIMEOUT = 60
RPC_MESSAGE_TTL = 60000

OK = HTTPStatus.OK
SERVICE_NOT_FOUND = HTTPStatus.NOT_FOUND
METHOD_NOT_FOUND = HTTPStatus.METHOD_NOT_ALLOWED
CALL_ERROR = HTTPStatus.INTERNAL_SERVER_ERROR
CALL_ARGS_MISMATCH = HTTPStatus.BAD_REQUEST
