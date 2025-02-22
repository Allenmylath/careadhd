from typing import Awaitable, Callable, Optional
from pydantic import BaseModel
import asyncio
import boto3
from botocore.client import BaseClient
import time
import io
import wave

from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    Frame,
    InputAudioRawFrame,
    OutputAudioRawFrame,
    StartFrame,
    StartInterruptionFrame,
)
from pipecat.serializers.base_serializer import FrameSerializer
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.processors.frame_processor import FrameDirection

from loguru import logger


class APIGatewayTransportParams(TransportParams):
    add_wav_header: bool = False
    serializer: FrameSerializer = TwilioFrameSerializer(stream_sid="default")
    api_gateway_endpoint: str
    connection_id: str  # Add connection_id as a required parameter

class APIGatewayInputTransport(BaseInputTransport):
    def __init__(
        self,
        params: APIGatewayTransportParams,
        apigw_client: BaseClient,
        callbacks: APIGatewayCallbacks,
        **kwargs,
    ):
        super().__init__(params, **kwargs)
        self._params = params
        self._apigw_client = apigw_client
        self._callbacks = callbacks

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self._callbacks.on_client_connected()

    async def handle_client_message(self, connection_id: str, message: bytes):
        """Handle incoming message from API Gateway"""
        if connection_id != self._params.connection_id:
            logger.warning(f"Received message from wrong connection: {connection_id}")
            return

        frame = self._params.serializer.deserialize(message)
        if not frame:
            return

        if isinstance(frame, InputAudioRawFrame):
            await self.push_audio_frame(frame)
        else:
            await self.push_frame(frame)

class APIGatewayOutputTransport(BaseOutputTransport):
    def __init__(
        self, params: APIGatewayTransportParams, apigw_client: BaseClient, **kwargs
    ):
        super().__init__(params, **kwargs)
        self._params = params
        self._apigw_client = apigw_client
        self._send_interval = (
            self._audio_chunk_size / self._params.audio_out_sample_rate
        ) / 2
        self._next_send_time = 0

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, StartInterruptionFrame):
            await self._send_frame(frame)
            self._next_send_time = 0

    async def write_raw_audio_frames(self, frames: bytes):
        """Send audio frames through API Gateway with timing control"""
        try:
            frame = OutputAudioRawFrame(
                audio=frames,
                sample_rate=self._params.audio_out_sample_rate,
                num_channels=self._params.audio_out_channels,
            )

            if self._params.add_wav_header:
                with io.BytesIO() as buffer:
                    with wave.open(buffer, "wb") as wf:
                        wf.setsampwidth(2)
                        wf.setnchannels(frame.num_channels)
                        wf.setframerate(frame.sample_rate)
                        wf.writeframes(frame.audio)
                    wav_frame = OutputAudioRawFrame(
                        buffer.getvalue(),
                        sample_rate=frame.sample_rate,
                        num_channels=frame.num_channels,
                    )
                    frame = wav_frame

            # Send the frame
            serialized = self._params.serializer.serialize(frame)
            if serialized:
                await self._send_to_connection(
                    serialized.encode() if isinstance(serialized, str) else serialized
                )

            # Simulate audio playback timing
            await self._write_audio_sleep()

        except Exception as e:
            logger.error(f"Error sending frame: {e}")
            raise

    async def _write_audio_sleep(self):
        """Simulate audio playback timing"""
        current_time = time.monotonic()
        sleep_duration = max(0, self._next_send_time - current_time)
        await asyncio.sleep(sleep_duration)
        if sleep_duration == 0:
            self._next_send_time = time.monotonic() + self._send_interval
        else:
            self._next_send_time += self._send_interval

    async def _send_frame(self, frame: Frame):
        """Send a frame through API Gateway"""
        serialized = self._params.serializer.serialize(frame)
        if serialized:
            await self._send_to_connection(
                serialized.encode() if isinstance(serialized, str) else serialized
            )

    async def _send_to_connection(self, data: bytes):
        """Send data through API Gateway"""
        try:
            await asyncio.to_thread(
                self._apigw_client.post_to_connection,
                ConnectionId=self._params.connection_id,
                Data=data,
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
        self._apigw_client = boto3.client(
            "apigatewaymanagementapi", endpoint_url=params.api_gateway_endpoint
        )

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
                callbacks=self._callbacks,
                name=self._input_name,
            )
        return self._input

    def output(self) -> APIGatewayOutputTransport:
        if not self._output:
            self._output = APIGatewayOutputTransport(
                self._params, self._apigw_client, name=self._output_name
            )
        return self._output

    async def _on_client_connected(self):
        await self._call_event_handler("on_client_connected")

    async def _on_client_disconnected(self):
        await self._call_event_handler("on_client_disconnected")
