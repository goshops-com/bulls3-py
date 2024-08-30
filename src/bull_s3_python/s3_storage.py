import json
import time
import uuid
import boto3
from botocore.exceptions import ClientError

class S3Storage:
    def __init__(self, config):
        self.s3 = boto3.client('s3')
        self.bucket_name = config['bucket_name']

    async def initialize(self):
        # S3 doesn't require initialization like Azure Blob Storage
        pass

    async def ensure_metadata_exists(self, queue_name):
        metadata_key = self._get_metadata_key(queue_name)
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=metadata_key)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                initial_metadata = {
                    "waitingJobs": [],
                    "inProgressJobs": [],
                    "completedJobs": [],
                    "failedJobs": []
                }
                self.s3.put_object(Bucket=self.bucket_name, Key=metadata_key, Body=json.dumps(initial_metadata))
            else:
                raise

    async def add_job(self, queue_name, job_data):
        await self.ensure_metadata_exists(queue_name)

        job_id = f"job_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        job_data['status'] = 'waiting'
        self.s3.put_object(Bucket=self.bucket_name, Key=f"{queue_name}/{job_id}.json", Body=json.dumps(job_data))

        await self._update_queue_metadata(queue_name, lambda metadata: {
            **metadata,
            "waitingJobs": metadata["waitingJobs"] + [job_id]
        })

        return job_id

    async def get_next_job(self, queue_name):
        await self.ensure_metadata_exists(queue_name)
        metadata = await self._get_queue_metadata(queue_name)

        for job_id in metadata["waitingJobs"]:
            try:
                job_data = await self.get_job_data(queue_name, job_id)
                if job_data['status'] == 'waiting':
                    job_data['status'] = 'active'
                    self.s3.put_object(Bucket=self.bucket_name, Key=f"{queue_name}/{job_id}.json", Body=json.dumps(job_data))

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
        self.s3.put_object(Bucket=self.bucket_name, Key=f"{queue_name}/{job_id}.json", Body=json.dumps(job_data))

        await self._update_queue_metadata(queue_name, lambda metadata: {
            **metadata,
            "inProgressJobs": [id for id in metadata["inProgressJobs"] if id != job_id],
            "completedJobs": metadata["completedJobs"] + [{"id": job_id, "completedAt": int(time.time())}] if job_data['status'] == 'completed' else metadata["completedJobs"],
            "failedJobs": metadata["failedJobs"] + [job_id] if job_data['status'] == 'failed' else metadata["failedJobs"]
        })

    async def get_job_data(self, queue_name, job_id):
        response = self.s3.get_object(Bucket=self.bucket_name, Key=f"{queue_name}/{job_id}.json")
        return json.loads(response['Body'].read().decode('utf-8'))

    async def _get_queue_metadata(self, queue_name):
        metadata_key = self._get_metadata_key(queue_name)
        response = self.s3.get_object(Bucket=self.bucket_name, Key=metadata_key)
        return json.loads(response['Body'].read().decode('utf-8'))

    async def _update_queue_metadata(self, queue_name, update_fn):
        metadata_key = self._get_metadata_key(queue_name)

        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                metadata = await self._get_queue_metadata(queue_name)
                updated_metadata = update_fn(metadata)
                self.s3.put_object(Bucket=self.bucket_name, Key=metadata_key, Body=json.dumps(updated_metadata))
                return
            except Exception:
                if attempt == max_attempts - 1:
                    raise Exception("Failed to update queue metadata after max attempts")
                time.sleep(2 ** attempt)  # Exponential backoff

    def _get_metadata_key(self, queue_name):
        return f"{queue_name}-metadata.json"