from typing import Awaitable, Callable, Optional
from pydantic import BaseModel
import asyncio
import boto3
from botocore.client import BaseClient

from pipecat.frames.frames import (
    AudioRawFrame,
    CancelFrame,
    EndFrame,
    InputAudioRawFrame,
    StartFrame,
)
from pipecat.serializers.base_serializer import FrameSerializer
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport

from loguru import logger


class APIGatewayTransportParams(TransportParams):
    add_wav_header: bool = False
    audio_frame_size: int = 6400  # 200ms
    serializer: FrameSerializer = TwilioFrameSerializer(stream_sid="default")  # Changed from ProtobufFrameSerializer
    api_gateway_endpoint: str


class APIGatewayCallbacks(BaseModel):
    on_client_connected: Callable[[], Awaitable[None]]
    on_client_disconnected: Callable[[], Awaitable[None]]


class APIGatewayInputTransport(BaseInputTransport):
    def __init__(
        self,
        params: APIGatewayTransportParams,
        apigw_client: BaseClient,
        **kwargs,
    ):
        super().__init__(params, **kwargs)
        self._params = params
        self._apigw_client = apigw_client
        self._current_connection_id: Optional[str] = None

    async def start(self, frame: StartFrame):
        await super().start(frame)

    async def stop(self, frame: EndFrame):
        await super().stop(frame)
        self._current_connection_id = None

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)
        self._current_connection_id = None

    async def handle_client_message(self, connection_id: str, message: bytes):
        """Handle incoming message from API Gateway"""
        if connection_id != self._current_connection_id:
            logger.warning("Received message from non-active connection")
            return

        frame = self._params.serializer.deserialize(message)
        if not frame:
            return

        if isinstance(frame, AudioRawFrame):
            await self.push_audio_frame(
                InputAudioRawFrame(
                    audio=frame.audio,
                    sample_rate=frame.sample_rate,
                    num_channels=frame.num_channels,
                )
            )
        else:
            await self.push_frame(frame)


class APIGatewayOutputTransport(BaseOutputTransport):
    def __init__(self, params: APIGatewayTransportParams, apigw_client: BaseClient, **kwargs):
        super().__init__(params, **kwargs)
        self._params = params
        self._apigw_client = apigw_client
        self._current_connection_id: Optional[str] = None
        self._audio_buffer = bytes()

    async def set_client_connection(self, connection_id: Optional[str]):
        """Set or clear the current client connection"""
        if self._current_connection_id and connection_id:
            logger.warning("Only one client allowed, using new connection")
        self._current_connection_id = connection_id
        self._audio_buffer = bytes()

    async def write_raw_audio_frames(self, frames: bytes):
        """Send audio frames to the current client through API Gateway"""
        if not self._current_connection_id:
            return

        try:
            self._audio_buffer += frames
            while len(self._audio_buffer) >= self._params.audio_frame_size:
                frame = AudioRawFrame(
                    audio=self._audio_buffer[:self._params.audio_frame_size],
                    sample_rate=self._params.audio_out_sample_rate,
                    num_channels=self._params.audio_out_channels,
                )

                serialized = self._params.serializer.serialize(frame)
                if serialized:
                    await self._send_to_connection(
                        serialized.encode() if isinstance(serialized, str) else serialized
                    )

                self._audio_buffer = self._audio_buffer[self._params.audio_frame_size:]

        except Exception as e:
            logger.error(f"Error sending frame: {e}")
            await self.set_client_connection(None)

    async def _send_to_connection(self, data: bytes):
        """Send data through API Gateway"""
        if not self._current_connection_id:
            return

        try:
            await asyncio.to_thread(
                self._apigw_client.post_to_connection,
                ConnectionId=self._current_connection_id,
                Data=data
            )
        except Exception as e:
            logger.error(f"Failed to send to connection: {e}")
            raise


class APIGatewayTransport(BaseTransport):
    def __init__(
        self,
        params: APIGatewayTransportParams,
        input_name: Optional[str] = None,
        output_name: Optional[str] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        super().__init__(input_name=input_name, output_name=output_name, loop=loop)
        self._params = params
        
        # Initialize API Gateway client
        self._apigw_client = boto3.client('apigatewaymanagementapi',
            endpoint_url=params.api_gateway_endpoint)

        self._callbacks = APIGatewayCallbacks(
            on_client_connected=self._on_client_connected,
            on_client_disconnected=self._on_client_disconnected,
        )
        
        self._input: Optional[APIGatewayInputTransport] = None
        self._output: Optional[APIGatewayOutputTransport] = None

        # Register event handlers
        self._register_event_handler("on_client_connected")
        self._register_event_handler("on_client_disconnected")

    def input(self) -> APIGatewayInputTransport:
        if not self._input:
            self._input = APIGatewayInputTransport(
                self._params,
                self._apigw_client,
                name=self._input_name
            )
        return self._input

    def output(self) -> APIGatewayOutputTransport:
        if not self._output:
            self._output = APIGatewayOutputTransport(
                self._params,
                self._apigw_client,
                name=self._output_name
            )
        return self._output

    async def _on_client_connected(self):
        if self._output:
            await self._call_event_handler("on_client_connected")
        else:
            logger.error("APIGatewayTransport output is missing in the pipeline")

    async def _on_client_disconnected(self):
        if self._output:
            await self._output.set_client_connection(None)
            await self._call_event_handler("on_client_disconnected")
        else:
            logger.error("APIGatewayTransport output is missing in the pipeline")
