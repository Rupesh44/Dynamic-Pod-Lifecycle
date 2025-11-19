import pika
import redis
import time
import json
import logging
import uvicorn
import httpx
import asyncio
import os  # <--- Added for Env Vars
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

# --- Configuration ---
LOGGING_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

# Load Config from Environment (Secrets)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq-service")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASSWORD", "admin")

REDIS_HOST = os.getenv("REDIS_HOST", "redis-master")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "admin")

# --- Global Connections ---
app = FastAPI()
redis_conn = None
http_client = None

# --- FastAPI Lifecycle Events ---

@app.on_event("startup")
def startup_event():
    """Connects to Redis and creates an HTTP client on startup."""
    global redis_conn, http_client
    
    logging.info(f"Connecting to Redis at {REDIS_HOST}...")
    while True:
        try:
            redis_conn = redis.Redis(host=REDIS_HOST, password=REDIS_PASSWORD, db=0, decode_responses=True)
            redis_conn.ping()
            logging.info("Connected to Redis successfully.")
            break
        except redis.exceptions.ConnectionError as e:
            logging.warning(f"Redis not ready, retrying in 5s... ({e})")
            time.sleep(5)
            
    http_client = httpx.AsyncClient(timeout=60.0)

@app.on_event("shutdown")
def shutdown_event():
    if http_client:
        asyncio.run(http_client.aclose())
    logging.info("Connections closed. Gateway shutting down.")

# --- Helper Functions ---

async def proxy_request(pod_ip: str, request: Request):
    target_url = f"http://{pod_ip}:80{request.url.path}"
    headers = {key: value for key, value in request.headers.items() if key.lower() != 'host'}
    headers['host'] = f"{pod_ip}" 
    
    try:
        req = http_client.build_request(
            method=request.method,
            url=target_url,
            headers=headers,
            params=request.query_params,
            content=request.stream()
        )
        resp = await http_client.send(req, stream=True)
        return StreamingResponse(
            resp.aiter_raw(),
            status_code=resp.status_code,
            headers=resp.headers
        )
    except (httpx.ConnectError, httpx.RequestError) as e:
        logging.error(f"Failed to connect to {target_url}: {e}")
        return Response(content="Session pod not reachable.", status_code=503)

def publish_creation_request(user_id):
    """Publishes a pod creation message using a fresh connection."""
    message_body = json.dumps({"id": user_id})
    connection = None
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST,
                                      credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS))
        )
        channel = connection.channel()
        channel.queue_declare(queue='pod_creation_queue', durable=True)
        
        channel.basic_publish(
            exchange='',
            routing_key='pod_creation_queue',
            body=message_body,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        logging.info(f"Published pod creation request for: {user_id}")
        
    except Exception as e:
        logging.error(f"Failed to publish message: {e}")
        raise e 
    finally:
        if connection and not connection.is_closed:
            connection.close()

# --- Main API Endpoint ---

@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def handle_all_requests(request: Request, full_path: str):
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        return Response(content="X-User-ID header is required.", status_code=400)

    session_key = f"session:{user_id}"
    
    try:
        session_data = redis_conn.hgetall(session_key)
        status = session_data.get("status")
        
        # --- SCENARIO 1: Session is Ready ---
        if status == "ready":
            pod_ip = session_data.get("ip")
            if not pod_ip:
                 return Response(content="Session 'ready' but no IP. Please retry.", status_code=500)
            
            redis_conn.hset(session_key, "last_active", int(time.time()))
            return await proxy_request(pod_ip, request)

        # --- SCENARIO 2: Session is Creating (Long Poll) ---
        if status == "initiating":
            logging.info(f"Session initiating for {user_id}. Starting long poll (Max 90s)...")
            timeout_start = time.time()
            
            while (time.time() - timeout_start) < 90: 
                await asyncio.sleep(0.5) 
                
                new_status = redis_conn.hget(session_key, "status")
                if new_status == "ready":
                    pod_ip = redis_conn.hget(session_key, "ip")
                    logging.info(f"Long poll success for {user_id}. Proxying to {pod_ip}...")
                    redis_conn.hset(session_key, "last_active", int(time.time()))
                    return await proxy_request(pod_ip, request)
                
                if new_status == "failed":
                    return Response(content="Pod creation failed. Please try again.", status_code=500)
            
            return Response(content="Gateway timeout: Pod creation is taking too long.", status_code=504)

        # --- SCENARIO 3: New Session ---
        if status is None:
            logging.info(f"New session for {user_id}. Triggering creation...")
            redis_conn.hset(session_key, "status", "initiating")
            
            try:
                publish_creation_request(user_id)
            except Exception:
                redis_conn.delete(session_key)
                return Response(content="Failed to queue pod creation.", status_code=500)
            
            return await handle_all_requests(request, full_path)
            
        # --- SCENARIO 4: Failed Session ---
        if status == "failed":
            logging.warning(f"Retrying failed session for {user_id}...")
            redis_conn.delete(session_key) 
            return await handle_all_requests(request, full_path)

    except redis.exceptions.ConnectionError:
        return Response(content="Gateway service is down: Cannot connect to Redis.", status_code=503)
    except Exception as e:
        logging.error(f"Unhandled error in gateway: {e}")
        return Response(content=f"An internal server error occurred: {e}", status_code=500)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)