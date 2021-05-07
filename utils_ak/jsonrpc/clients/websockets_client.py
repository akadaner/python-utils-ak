import time
import asyncio
from utils_ak.coder import JsonCoder
from utils_ak.id_generator import IncrementalIDGenerator


class WebSocketsClient:
    def __init__(self, websocket_client, id_generator=None):
        self.websocket_client = websocket_client
        self.responses = {}  # {<id>: response}
        self.coder = JsonCoder(encoding=None)

        self.active = True
        self.id_generator = id_generator or IncrementalIDGenerator()

    def _prepare_request(self, method, generate_id=True, **kwargs):
        request = {"jsonrpc": "2.0", "params": kwargs, "method": method}
        if generate_id:
            request["id"] = self.id_generator.gen_id()
        return request

    async def execute(self, method, notify=False, **kwargs):
        """
        :param request: {"jsonrpc": "2.0", "method": "post.like", "params": {"post": "12345"}, "id": 1}
        """
        request = self._prepare_request(method, generate_id=not notify, **kwargs)
        await self.websocket_client.send(self.coder.encode(request))

        if not notify:
            while True:
                if request["id"] in self.responses:
                    return self.responses.pop(request["id"])
                await asyncio.sleep(0.001)

    async def start_receiving_loop(self):
        while True:
            if not self.active:
                return

            try:
                response_text = await asyncio.wait_for(
                    self.websocket_client.recv(), timeout=0.1
                )
            except asyncio.TimeoutError:
                await asyncio.sleep(0)
                continue
            data = self.coder.decode(response_text)
            self.responses[data["id"]] = data
            await asyncio.sleep(0)

    def stop_receiving_loop(self):
        self.active = False
