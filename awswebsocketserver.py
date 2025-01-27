import asyncio
import json
from typing import Any, Dict, Optional
from loguru import logger

from pipecat.frames.frames import (
    EndFrame,
    Frame,
    InputAudioRawFrame,
    StartFrame,
    StartInterruptionFrame,
)


from pipecat.transports.network.websocket_server import (
    WebsocketServerTransport,
    WebsocketServerParams,
    WebsocketServerInputTransport,
    WebsocketServerOutputTransport,
)
from pipecat.processors.frame_processor import FrameDirection


class LambdaWebSocketParams(WebsocketServerParams):
    """Parameters specific to Lambda WebSocket transport"""

    connection_id: str
    api_client: Any  # boto3 API Gateway Management API client


class MockWebSocket:
    """Mock WebSocket class to simulate websockets.WebSocketServerProtocol"""

    def __init__(self, connection_id: str, api_client: Any):
        self.connection_id = connection_id
        self.api_client = api_client
        self.closed = False
        self.remote_address = f"lambda-{connection_id}"

    async def send(self, data: str | bytes):
        """Send data through API Gateway WebSocket"""
        try:
            await self.api_client.post_to_connection(
                ConnectionId=self.connection_id,
                Data=data if isinstance(data, bytes) else data.encode(),
            )
        except Exception as e:
            logger.error(f"Error sending data: {str(e)}")
            raise

    async def close(self):
        """Mark connection as closed"""
        self.closed = True


class LambdaInputTransport(WebsocketServerInputTransport):
    """Input transport adapted for Lambda"""

    def __init__(self, params: LambdaWebSocketParams, **kwargs):
        super().__init__("", 0, params, None, **kwargs)  # Host and port not used
        self._lambda_params = params
        self._mock_websocket = MockWebSocket(params.connection_id, params.api_client)

    async def start(self, frame: StartFrame):
        """Start the transport - simpler than WebSocket server version"""
        await super().start(frame)
        self._websocket = self._mock_websocket
        if self._callbacks:
            await self._callbacks.on_client_connected(self._websocket)

    async def handle_message(self, message: Dict[str, Any]):
        """Handle incoming message from Lambda"""
        frame = self._params.serializer.deserialize(message)
        if not frame:
            return

        if isinstance(frame, InputAudioRawFrame):
            await self.push_audio_frame(frame)
        else:
            await self.push_frame(frame)

    async def stop(self, frame: EndFrame):
        """Stop the transport"""
        if self._callbacks and self._websocket:
            await self._callbacks.on_client_disconnected(self._websocket)
        await super().stop(frame)


class LambdaOutputTransport(WebsocketServerOutputTransport):
    """Output transport adapted for Lambda"""

    def __init__(self, params: LambdaWebSocketParams, **kwargs):
        super().__init__(params, **kwargs)
        self._lambda_params = params
        self._mock_websocket = MockWebSocket(params.connection_id, params.api_client)
        self._websocket = self._mock_websocket

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Process a frame - similar to parent but adapted for Lambda"""
        await super().process_frame(frame, direction)
        if isinstance(frame, StartInterruptionFrame):
            await self._write_frame(frame)
            self._next_send_time = 0


class LambdaTransport(WebsocketServerTransport):
    """Main transport class for Lambda WebSocket implementation"""

    def __init__(
        self,
        ws_interface: Any,  # Your WebSocketInterface class
        params: LambdaWebSocketParams = None,
        input_name: Optional[str] = None,
        output_name: Optional[str] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        # Initialize with dummy host/port since we don't use them
        super().__init__(
            host="",
            port=0,
            params=params
            or LambdaWebSocketParams(
                connection_id=ws_interface.connection_id,
                api_client=ws_interface.api_client,
            ),
            input_name=input_name,
            output_name=output_name,
            loop=loop,
        )
        self._ws_interface = ws_interface
        self._lambda_params = self._params

    def input(self) -> LambdaInputTransport:
        """Create input transport if not exists"""
        if not self._input:
            self._input = LambdaInputTransport(
                self._lambda_params, name=self._input_name
            )
        return self._input

    def output(self) -> LambdaOutputTransport:
        """Create output transport if not exists"""
        if not self._output:
            self._output = LambdaOutputTransport(
                self._lambda_params, name=self._output_name
            )
        return self._output

    async def handle_lambda_event(self, event: Dict[str, Any]):
        """Handle incoming Lambda WebSocket event"""
        if "body" in event:
            try:
                body = json.loads(event["body"])
                if self._input:
                    await self._input.handle_message(body)
            except Exception as e:
                logger.error(f"Error handling lambda event: {str(e)}")
