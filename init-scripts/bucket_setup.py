import time
import socket
import logging
from minio import Minio
from minio.error import S3Error
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class MinioManager:
    def __init__(self, buckets=None):
        self.host = os.getenv("MINIO_HOST")
        self.port = os.getenv("MINIO_PORT")
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        
        if not self.access_key or not self.secret_key:
            raise ValueError("Missing MINIO_ROOT_USER or MINIO_ROOT_PASSWORD")

        self.client = None
        self._init_client()


    def _init_client(self):
        logging.info("Initializing Minio client...")
        try:
            self._wait_for_service()
            self.client = Minio(
                f"{self.host}:{self.port}",
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=False
            )
            logging.info("Minio client successfully created.")
        except Exception as e:
            logging.error(f"Failed to initialize Minio client: {e}")
            raise


    def _wait_for_service(self, timeout=30, retries=3, sleep=5):
        for attempt in range(1, retries + 1):
            try:
                with socket.create_connection((self.host, self.port), timeout=timeout):
                    logging.info(f"Service available at {self.host}:{self.port}")
                    return
            except OSError:
                logging.warning(f"Waiting for service {self.host}:{self.port}... attempt {attempt}/{retries}")
                time.sleep(sleep)
        raise TimeoutError(f"Service {self.host}:{self.port} unavailable after {retries} attempts")


    def create_buckets(self, buckets: list[str]):
        if not self.client:
            raise RuntimeError("Minio client is not initialized.")
        for bucket in buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logging.info(f"Bucket '{bucket}' created.")
                else:
                    logging.info(f"Bucket '{bucket}' already exists.")
            except S3Error as e:
                logging.error(f"Failed to create bucket '{bucket}': {e}")

 
if __name__ == "__main__":
    lakehouse_bucket = os.getenv("MINIO_BUCKET")

    manager = MinioManager()
    manager.create_buckets(buckets=[lakehouse_bucket])
