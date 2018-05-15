from gevent import monkey  # isort:skip
monkey.patch_all()  # isort:skip

import timeit  # noqa: E402

from simple_amqp import AmqpParameters  # noqa: E402
from simple_amqp_rpc import Service  # noqa: E402
from simple_amqp_rpc.gevent import GeventAmqpRpc  # noqa: E402

rpc_conn = GeventAmqpRpc(
    AmqpParameters(),
    'pong',
)

PingClient = rpc_conn.client('ping', 'ping')


class PongService:
    svc = Service('pong')

    @svc.rpc
    def pong(self, name: str):
        resp = PingClient.ping(name)
        return 'resp [{}]: {}'.format(
            resp.status,
            resp.body,
        )


pong_service = PongService()

rpc_conn \
    .add_svc(pong_service)

rpc_conn.configure()
rpc_conn.start()
print('## started')

while True:
    print('## req')
    start = timeit.default_timer()
    print(pong_service.pong('duck'))
    end = timeit.default_timer()
    print('## dt {0:.2f}ms'.format((end - start) * 1000))
    print('## resp')
