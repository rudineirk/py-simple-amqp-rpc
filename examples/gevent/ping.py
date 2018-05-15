from gevent import monkey  # isort:skip
monkey.patch_all()  # isort:skip

from gevent import sleep  # noqa: E402

from simple_amqp import AmqpParameters  # noqa: E402
from simple_amqp_rpc import Service  # noqa: E402
from simple_amqp_rpc.gevent import GeventAmqpRpc  # noqa: E402

rpc_conn = GeventAmqpRpc(
    AmqpParameters(),
    'ping',
)


class PingService:
    svc = Service('ping')

    @svc.rpc
    def ping(self, name: str):
        return 'pong: {}'.format(name)


ping_service = PingService()

rpc_conn \
    .add_svc(ping_service)

rpc_conn.configure()
rpc_conn.start()

while True:
    sleep(1)
