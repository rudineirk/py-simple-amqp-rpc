from asyncio import get_event_loop, set_event_loop_policy, sleep

import uvloop

from simple_amqp import AmqpParameters
from simple_amqp_rpc import Service
from simple_amqp_rpc.asyncio import AsyncioAmqpRpc

set_event_loop_policy(uvloop.EventLoopPolicy())


class PingService:
    svc = Service('ping')

    @svc.rpc
    async def ping(self, name: str):
        return 'pong: {}'.format(name)


rpc_conn = AsyncioAmqpRpc(
    AmqpParameters(),
    'ping',
)

ping_service = PingService()
rpc_conn \
    .add_svc(ping_service)

rpc_conn.configure()

loop = get_event_loop()


async def main():
    await rpc_conn.start()

    while True:
        await sleep(1)

loop.run_until_complete(main())
