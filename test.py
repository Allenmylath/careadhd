import websockets
import asyncio
import json


async def connect_websocket(url: str):
    try:
        # Increase timeout to 30 seconds and disable ping/pong
        websocket = await websockets.connect(
            url, ping_interval=None, close_timeout=30, open_timeout=30
        )
        print("Connected to WebSocket server")
        return websocket
    except asyncio.TimeoutError:
        print("Connection timed out - the server did not respond")
        return None
    except websockets.exceptions.InvalidStatusCode as e:
        print(f"Server rejected connection with status code: {e.status_code}")
        return None
    except ConnectionRefusedError:
        print("Connection refused - the server actively rejected the connection")
        return None
    except Exception as e:
        print(f"Connection failed with error: {type(e).__name__} - {str(e)}")
        return None


async def send_message(websocket, action: str, data: dict = None):
    try:
        message = {"action": action, "data": data or {}}
        await websocket.send(json.dumps(message))
        print(f"Sent message: {message}")

        # Wait for response
        response = await websocket.recv()
        print(f"Received: {response}")
    except Exception as e:
        print(f"Error sending/receiving message: {str(e)}")


async def disconnect_websocket(websocket):
    if websocket:
        try:
            await websocket.close()
            print("Disconnected from WebSocket server")
        except Exception as e:
            print(f"Error during disconnect: {str(e)}")


async def main():
    url = "wss://x9mernly8h.execute-api.us-east-1.amazonaws.com/production"

    print(f"Attempting to connect to {url}")
    websocket = await connect_websocket(url)

    if websocket:
        try:
            print("Connection successful, sending test message...")
            # Send a message that will route to the default handler
            await send_message(websocket, "default", {"message": "Hello server!"})

            print("Waiting for 5 seconds...")
            await asyncio.sleep(5)
        finally:
            await disconnect_websocket(websocket)
    else:
        print("Failed to establish connection")


if __name__ == "__main__":
    asyncio.run(main())
