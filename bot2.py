import os
import sys
from typing import Optional

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import EndFrame, LLMMessagesFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.services.cartesia import CartesiaTTSService
from pipecat.services.openai import OpenAILLMService
from pipecat.services.gladia import GladiaSTTService
from pipecat.audio.filters.noisereduce_filter import NoisereduceFilter

from loguru import logger
from dotenv import load_dotenv

from apigwtransport import (
    APIGatewayTransport,
    APIGatewayTransportParams,
)

from text import text

load_dotenv(override=True)

logger.remove(0)
logger.add(sys.stderr, level="DEBUG")

async def run_bot(
    transport: APIGatewayTransport,
    connection_id: Optional[str] = None,
    stream_sid: str = "default",
):
    """Run the bot with API Gateway interface
    
    Args:
        transport: Transport instance for handling API Gateway communication
        connection_id: Optional connection ID for the API Gateway
        stream_sid: Stream ID for the serializer, defaults to "default"
    """
    llm = OpenAILLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        model="gpt-4"
    )

    stt = GladiaSTTService(
        api_key=os.getenv("GLADIA_API_KEY"),
    )

    tts = CartesiaTTSService(
        api_key=os.getenv("CARTESIA_API_KEY"),
        voice_id="79a125e8-cd45-4c13-8a67-188112f4dd22",  # British Lady
        sample_rate=16000,
    )

    messages = [
        {
            "role": "system",
            "content": (
                "You are a helpful assistant named Jessica at CARE ADHD. "
                "Your output will be converted to audio, so avoid using special characters in your answers. "
                "Don't give long responses as user may get bored hearing long speech from converted audio. "
                "You should be warm and supportive while maintaining professional boundaries. "
                "You can assist with: "
                "General information about ADHD support programs, "
                "Basic service inquiries, "
                "Educational resource connections, "
                "Simple scheduling tasks. "
                "You must not provide medical advice or discuss personal health details. "
                "For any clinical questions or specific medical concerns, inform users that a qualified "
                "healthcare professional from the care team will contact them directly. "
                "Respond to users in a creative and helpful way, keeping your tone warm but professional. "
                "Focus on administrative and informational support only. "
                "When medical questions arise, gracefully transition to arranging contact with a human healthcare provider. "
                "When questions unrelated to ADHD are asked gracefully transition to your core purpose. "
                "Always remember your responses will be converted to audio, so maintain clear, natural speech patterns "
                "and AVOID TECHNICAL FORMATTING AND SPECIAL CHARACTERS." + text
            ),
        }
    ]

    context = OpenAILLMContext(messages)
    context_aggregator = llm.create_context_aggregator(context)

    pipeline = Pipeline(
        [
            transport.input(),  # API Gateway input from client
            stt,  # Speech-To-Text
            context_aggregator.user(),
            llm,  # LLM
            tts,  # Text-To-Speech
            transport.output(),  # API Gateway output to client
            context_aggregator.assistant(),
        ]
    )

    task = PipelineTask(pipeline, params=PipelineParams(allow_interruptions=True))

    @transport.event_handler("on_client_connected")
    async def on_client_connected():
        # Kick off the conversation
        messages.append(
            {"role": "system", "content": "Please introduce yourself to the user."}
        )
        await task.queue_frames([LLMMessagesFrame(messages)])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected():
        await task.queue_frames([EndFrame()])

    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)
