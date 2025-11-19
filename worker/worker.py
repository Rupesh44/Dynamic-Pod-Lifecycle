import pika
import redis
import time
import json
import logging
import re
import os # <--- Added
from kubernetes import client, config, watch

# --- Configuration ---
LOGGING_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

# Load Config from Environment
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq-service")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASSWORD", "admin")

REDIS_HOST = os.getenv("REDIS_HOST", "redis-master")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "admin")

# Namespace for User Pods (Defaults to default, can be changed via Env)
USER_POD_NAMESPACE = os.getenv("USER_POD_NAMESPACE", "default")

# --- Helper Functions ---

def sanitize_k8s_name(text):
    safe_text = text.lower()
    safe_text = re.sub(r'[^a-z0-9]', '-', safe_text)
    safe_text = safe_text.strip('-')
    return safe_text if safe_text else "anonymous"

def get_pod_manifest(user_id, safe_id):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"session-{safe_id}",
            "namespace": USER_POD_NAMESPACE, # Uses the dynamic namespace
            "labels": {"app": "session-pod", "user_id": safe_id},
            "annotations": {"original_id": user_id}
        },
        "spec": {
            "containers": [{
                "name": "web-container",
                "image": "httpd:2.4-alpine",
                "ports": [{"containerPort": 80}]
            }],
            "restartPolicy": "Never"
        }
    }

def wait_for_pod_ready(k8s_api, pod_name):
    """Waits for Pod IP in the configured namespace."""
    w = watch.Watch()
    for event in w.stream(k8s_api.list_namespaced_pod, namespace=USER_POD_NAMESPACE, field_selector=f"metadata.name={pod_name}", timeout_seconds=60):
        pod = event['object']
        if pod.status.phase == "Running" and pod.status.pod_ip:
            w.stop()
            return pod.status.pod_ip
    return None

# --- Main Callback ---

def on_message_callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        user_id = data.get("id")
        if not user_id: 
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        session_key = f"session:{user_id}"
        safe_id = sanitize_k8s_name(user_id)
        pod_name = f"session-{safe_id}"

        logging.info(f"Processing: {user_id} -> Pod: {pod_name} in NS: {USER_POD_NAMESPACE}")

        k8s_api = client.CoreV1Api()

        # Check if exists
        try:
            pod = k8s_api.read_namespaced_pod(name=pod_name, namespace=USER_POD_NAMESPACE)
            if pod.status.phase == "Running":
                 redis_conn.hset(session_key, mapping={"status": "ready", "ip": pod.status.pod_ip, "last_active": int(time.time())})
        except client.rest.ApiException as e:
            if e.status == 404:
                # Create
                k8s_api.create_namespaced_pod(namespace=USER_POD_NAMESPACE, body=get_pod_manifest(user_id, safe_id))
                pod_ip = wait_for_pod_ready(k8s_api, pod_name)
                
                if pod_ip:
                    redis_conn.hset(session_key, mapping={"status": "ready", "ip": pod_ip, "last_active": int(time.time())})
                    logging.info(f"Ready: {user_id}")
                else:
                    redis_conn.hset(session_key, "status", "failed")

    except Exception as e:
        logging.error(f"Error: {e}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    logging.info("Starting Lifecycle Worker Service...")
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config() 
    
    global redis_conn
    logging.info(f"Connecting to Redis at {REDIS_HOST}...")
    while True:
        try:
            redis_conn = redis.Redis(host=REDIS_HOST, password=REDIS_PASSWORD, db=0, decode_responses=True)
            redis_conn.ping()
            logging.info("Connected to Redis successfully.")
            break
        except Exception:
            time.sleep(5)

    logging.info(f"Connecting to RabbitMQ at {RABBITMQ_HOST}...")
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)))
            channel = connection.channel()
            channel.queue_declare(queue='pod_creation_queue', durable=True)
            channel.basic_consume(queue='pod_creation_queue', on_message_callback=on_message_callback)
            channel.start_consuming()
        except Exception:
            time.sleep(5)

if __name__ == "__main__":
    main()