import asyncio
import websockets
import time
from icecream import ic

from utils_ak.jsonrpc.clients.websockets_client import WebSocketsClient

# from jsonrpcclient.clients.websockets_client import WebSocketsClient


loop = asyncio.get_event_loop()


async def main():
    async with websockets.connect("ws://localhost:5000") as ws:
        client = WebSocketsClient(ws)

        asyncio.ensure_future(client.start_receive_loop())

        for i in range(5):
            response = await client.request("ping")
            ic(response)
            time.sleep(3)


asyncio.ensure_future(main())
loop.run_forever()
