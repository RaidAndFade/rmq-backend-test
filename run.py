import asyncio
import os
import aio_pika
import environ
import pickle
env = environ.Env()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

class RPCServer:
    def __init__(self):
        self.endpoints = {}

    async def init(self, backend_name):
        self.connection = await aio_pika.connect_robust(env("RABBITMQ_URL"))
        self.loop = asyncio.get_event_loop()
        self.channel = await self.connection.channel()
        self.message_exchange = await self.channel.declare_exchange(backend_name)
        self.request_queue = await self.channel.declare_queue("rpc_queue")
        await self.request_queue.bind(self.message_exchange,"rpc_queue")
        await self.request_queue.consume(self.on_request)

    async def on_request(self, message):
        with message.process():
            error=response=None
            body = pickle.loads(message.body)
            if body['endpoint'] in self.endpoints:
                response=self.endpoints[body['endpoint']](request=body['request'],**body['params'])
            else:
                error="No endpoint found"

            await self.message_exchange.publish(
                aio_pika.Message(
                    body=pickle.dumps({
                        'error':error,
                        'response':response
                    }),
                    correlation_id=message.correlation_id
                ),
                routing_key=message.reply_to,
            )
            print(f"[{message.correlation_id}] Sent response {response}.")

def echo_back(request,text):
    return f"{text} received from {request['remote_addr']}! Sending back :)"

async def main():
    print("Initializing RMQ Listener")

    rpcs = RPCServer()
    rpcs.endpoints['echo_back'] = echo_back

    print("Added echo_back endpoint")

    await rpcs.init("hello_world")
    print("Initialized hello_world listener.")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
