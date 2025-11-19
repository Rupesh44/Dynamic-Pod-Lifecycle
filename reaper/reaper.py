import redis
import time
import logging
import os # <--- Added
from kubernetes import client, config

# --- Configuration ---
LOGGING_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

REDIS_HOST = os.getenv("REDIS_HOST", "redis-master")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "admin")
IDLE_TIMEOUT_SECONDS = 600 

# Namespace where User Pods live
USER_POD_NAMESPACE = os.getenv("USER_POD_NAMESPACE", "default")

# --- Main Reaper Logic ---

def main():
    logging.info("Starting Reaper Service...")
    
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config() 
    
    k8s_api = client.CoreV1Api()

    logging.info(f"Connecting to Redis at {REDIS_HOST}...")
    while True:
        try:
            redis_conn = redis.Redis(host=REDIS_HOST, password=REDIS_PASSWORD, db=0, decode_responses=True)
            redis_conn.ping()
            logging.info("Connected to Redis successfully.")
            break
        except redis.exceptions.ConnectionError as e:
            logging.warning(f"Redis not ready... ({e})")
            time.sleep(5)

    logging.info(f"Reaper started. Watching namespace: {USER_POD_NAMESPACE}. Timeout: {IDLE_TIMEOUT_SECONDS}s.")
    while True:
        try:
            session_keys = redis_conn.keys("session:*")
            
            if not session_keys:
                logging.info("No active sessions found. Sleeping...")
            else:
                logging.info(f"Checking {len(session_keys)} active sessions...")

            current_time = int(time.time())

            for key in session_keys:
                try:
                    last_active_str = redis_conn.hget(key, "last_active")
                    
                    if not last_active_str:
                        # If it's been > 90s and still no last_active, it's a stale creating session
                        continue
                        
                    last_active = int(last_active_str)
                    idle_time = current_time - last_active

                    if idle_time > IDLE_TIMEOUT_SECONDS:
                        # --- IDLE POD DETECTED ---
                        user_id = key.split(":")[-1]
                        
                        # We need to sanitize the ID to find the pod name
                        # (Simple sanitization match for deletion logic)
                        import re
                        safe_id = re.sub(r'[^a-z0-9]', '-', user_id.lower()).strip('-')
                        pod_name = f"session-{safe_id}"
                        
                        logging.info(f"Session {user_id} is idle for {idle_time}s. TRIGGERING REAP.")
                        
                        # 1. Delete the Kubernetes Pod from the correct namespace
                        try:
                            k8s_api.delete_namespaced_pod(
                                name=pod_name,
                                namespace=USER_POD_NAMESPACE,
                                body=client.V1DeleteOptions()
                            )
                            logging.info(f"Deleted pod: {pod_name}")
                        except client.rest.ApiException as e:
                            if e.status == 404:
                                logging.warning(f"Pod {pod_name} already deleted.")
                            else:
                                logging.error(f"Failed to delete pod {pod_name}: {e}")
                                continue 

                        # 2. Delete the Redis Key
                        redis_conn.delete(key)
                        logging.info(f"Deleted Redis key: {key}")
                    
                except Exception as e:
                    logging.error(f"Error processing key {key}: {e}")

        except Exception as e:
            logging.error(f"Main loop error: {e}. Reconnecting...")
            time.sleep(10)
            main() 

        time.sleep(60)

if __name__ == "__main__":
    main()