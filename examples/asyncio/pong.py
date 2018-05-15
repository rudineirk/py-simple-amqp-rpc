import timeit
from asyncio import get_event_loop, set_event_loop_policy

import uvloop

from simple_amqp import AmqpParameters
from simple_amqp_rpc import Service
from simple_amqp_rpc.asyncio import AsyncioAmqpRpc

set_event_loop_policy(uvloop.EventLoopPolicy())

rpc_conn = AsyncioAmqpRpc(
    AmqpParameters(),
    'pong',
)

PingClient = rpc_conn.client('ping', 'ping')


class PongService:
    svc = Service('pong')

    @svc.rpc
    async def pong(self, name: str):
        resp = await PingClient.ping(name)
        return 'resp [{}]: {}'.format(
            resp.status,
            resp.body,
        )


pong_service = PongService()
rpc_conn \
    .add_svc(pong_service)

rpc_conn.configure()

loop = get_event_loop()


async def main():
    await rpc_conn.start()
    print('## started')

    while True:
        print('## req')
        start = timeit.default_timer()
        print(await pong_service.pong('duck'))
        end = timeit.default_timer()
        print('## dt {0:.2f}ms'.format((end - start) * 1000))
        print('## resp')

loop.run_until_complete(main())
