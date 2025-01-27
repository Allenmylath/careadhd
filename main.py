import json
import boto3
from bot import run_bot
from typing import Dict, Any


def update_endpoint_url(event: Dict[Any, Any]) -> None:
    """Update the API Gateway endpoint URL for the WebSocket connection"""
    domain = event["requestContext"]["domainName"]
    stage = event["requestContext"]["stage"]
    endpoint_url = f"https://{domain}/{stage}"
    global api_client
    api_client = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)


class WebSocketInterface:
    """Interface to make the Lambda function work like a WebSocket"""

    def __init__(self, connection_id: str, api_client: Any):
        self.connection_id = connection_id
        self.api_client = api_client

    async def send(self, data: Any) -> None:
        """Send any data to the client"""
        try:
            await self.api_client.post_to_connection(
                ConnectionId=self.connection_id, Data=data
            )
        except Exception as e:
            print(f"Error sending data: {str(e)}")

    async def close(self) -> None:
        """Close the WebSocket connection"""
        pass  # No need to clean up anything since we're not tracking connections


def connect_handler(event: Dict[Any, Any], context: Any) -> Dict[str, Any]:
    """Handle WebSocket connect events"""
    return {"statusCode": 200, "body": "Connected"}


def disconnect_handler(event: Dict[Any, Any], context: Any) -> Dict[str, Any]:
    """Handle WebSocket disconnect events"""
    return {"statusCode": 200, "body": "Disconnected"}


async def message_handler(event: Dict[Any, Any], context: Any) -> Dict[str, Any]:
    """Handle WebSocket message events"""
    connection_id = event["requestContext"]["connectionId"]
    update_endpoint_url(event)

    try:
        body = json.loads(event["body"])

        # Handle start message
        if "start" in body:
            stream_sid = body["start"]["streamSid"]

            # Create a WebSocket-like interface for the bot
            ws_interface = WebSocketInterface(connection_id, api_client)

            # Run the bot
            await run_bot(ws_interface, stream_sid)

        return {"statusCode": 200, "body": "Message processed"}

    except Exception as e:
        return {"statusCode": 500, "body": f"Error processing message: {str(e)}"}


async def handler(event: Dict[Any, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler"""
    route_key = event["requestContext"]["routeKey"]

    if route_key == "$connect":
        return connect_handler(event, context)
    elif route_key == "$disconnect":
        return disconnect_handler(event, context)
    elif route_key == "$default":
        return message_handler(event, context)
    else:
        return {"statusCode": 400, "body": f"Unsupported route: {route_key}"}
