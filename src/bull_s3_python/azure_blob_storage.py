import json
import time
import uuid
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

class AzureBlobStorage:
    def __init__(self, config):
        self.blob_service_client = BlobServiceClient.from_connection_string(config['connection_string'])
        self.container_name = config['container_name']
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    async def initialize(self):
        await self.ensure_container_exists()

    async def ensure_container_exists(self):
        await self.container_client.create_container()

    async def ensure_metadata_exists(self, queue_name):
        metadata_key = self._get_metadata_key(queue_name)
        blob_client = self.container_client.get_blob_client(metadata_key)

        try:
            await blob_client.get_blob_properties()
        except Exception as error:
            if hasattr(error, 'status_code') and error.status_code == 404:
                initial_metadata = {
                    "waitingJobs": [],
                    "inProgressJobs": [],
                    "completedJobs": [],
                    "failedJobs": []
                }
                await blob_client.upload_blob(json.dumps(initial_metadata))
            else:
                raise error

    async def add_job(self, queue_name, job_data):
        await self.ensure_metadata_exists(queue_name)

        job_id = f"job_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        blob_client = self.container_client.get_blob_client(f"{queue_name}/{job_id}.json")
        job_data['status'] = 'waiting'
        await blob_client.upload_blob(json.dumps(job_data))

        await self._update_queue_metadata(queue_name, lambda metadata: {
            **metadata,
            "waitingJobs": metadata["waitingJobs"] + [job_id]
        })

        return job_id

    async def get_next_job(self, queue_name):
        await self.ensure_metadata_exists(queue_name)
        metadata = await self._get_queue_metadata(queue_name)

        for job_id in metadata["waitingJobs"]:
            blob_client = self.container_client.get_blob_client(f"{queue_name}/{job_id}.json")

            try:
                job_data = await self.get_job_data(queue_name, job_id)
                if job_data['status'] == 'waiting':
                    job_data['status'] = 'active'
                    await blob_client.upload_blob(json.dumps(job_data), overwrite=True)

                    await self._update_queue_metadata(queue_name, lambda metadata: {
                        **metadata,
                        "waitingJobs": [id for id in metadata["waitingJobs"] if id != job_id],
                        "inProgressJobs": metadata["inProgressJobs"] + [job_id]
                    })

                    return {"id": job_id, "data": job_data}
            except Exception:
                continue

        return None

    async def update_job(self, queue_name, job_id, job_data):
        blob_client = self.container_client.get_blob_client(f"{queue_name}/{job_id}.json")
        await blob_client.upload_blob(json.dumps(job_data), overwrite=True)

        await self._update_queue_metadata(queue_name, lambda metadata: {
            **metadata,
            "inProgressJobs": [id for id in metadata["inProgressJobs"] if id != job_id],
            "completedJobs": metadata["completedJobs"] + [{"id": job_id, "completedAt": int(time.time())}] if job_data['status'] == 'completed' else metadata["completedJobs"],
            "failedJobs": metadata["failedJobs"] + [job_id] if job_data['status'] == 'failed' else metadata["failedJobs"]
        })

    def _prune_completed_jobs(self, metadata):
        now = int(time.time())
        metadata["completedJobs"] = [
            job for job in metadata["completedJobs"]
            if (now - job["completedAt"]) < self.completed_job_retention_period
        ][-self.max_completed_jobs:]
        return metadata

    async def get_job_data(self, queue_name, job_id):
        blob_client = self.container_client.get_blob_client(f"{queue_name}/{job_id}.json")
        blob_data = await blob_client.download_blob()
        return json.loads(await blob_data.content_as_text())

    async def _get_queue_metadata(self, queue_name):
        metadata_key = self._get_metadata_key(queue_name)
        blob_client = self.container_client.get_blob_client(metadata_key)
        blob_data = await blob_client.download_blob()
        return json.loads(await blob_data.content_as_text())

    async def _update_queue_metadata(self, queue_name, update_fn):
        metadata_key = self._get_metadata_key(queue_name)
        blob_client = self.container_client.get_blob_client(metadata_key)

        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                metadata = await self._get_queue_metadata(queue_name)
                updated_metadata = update_fn(metadata)
                await blob_client.upload_blob(json.dumps(updated_metadata), overwrite=True)
                return
            except Exception:
                if attempt == max_attempts - 1:
                    raise Exception("Failed to update queue metadata after max attempts")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

    def _get_metadata_key(self, queue_name):
        return f"{queue_name}-metadata.json"