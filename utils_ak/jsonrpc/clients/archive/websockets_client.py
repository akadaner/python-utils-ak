from typing import Any, Optional, Iterator
from apply_defaults import apply_self
from jsonrpcclient.clients.websockets_client import (
    WebSocketsClient as _WebSocketsClient,
)
from jsonrpcclient.response import Response
from jsonrpcclient.requests import Request
from jsonrpcclient.parse import parse

from utils_ak.coder import JsonCoder

import time
import asyncio
from icecream import ic


class WebSocketsClient(_WebSocketsClient):
    """
    A wrapper for jsonrpcclient.websockets_client.WebSocketsClient with proper id matching.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.responses = {}  # {<id>: response}
        self.coder = JsonCoder()

    async def send_message(
        self, request: str, response_expected: bool, **kwargs: Any
    ) -> Response:
        """make response_expected=False for every call"""
        return await super().send_message(request, response_expected=False, **kwargs)

    @apply_self
    async def request(
        self,
        method_name: str,
        *args: Any,
        trim_log_values: bool = False,
        validate_against_schema: bool = True,
        id_generator: Optional[Iterator] = None,
        **kwargs: Any
    ) -> Response:
        request = Request(method_name, id_generator=id_generator, *args, **kwargs)

        id = str(request["id"])

        await self.send(
            request,
            trim_log_values=trim_log_values,
            validate_against_schema=validate_against_schema,
        )

        while True:
            if id in self.responses:
                return self.responses.pop(id)
            await asyncio.sleep(0.001)

    def stop_receiving_loop(self):
        self.responses = None

    async def start_receiving_loop(self):
        while True:
            if self.responses is None:
                return

            try:
                response_text = await asyncio.wait_for(self.socket.recv(), timeout=0.1)
            except asyncio.TimeoutError:
                await asyncio.sleep(0)
                continue
            data = self.coder.decode(response_text)
            id = str(data["id"])

            if self.responses is None:
                return

            self.responses[id] = data
            await asyncio.sleep(0)
