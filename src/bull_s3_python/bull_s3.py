import asyncio
from typing import Callable, Any

class BullS3:
    def __init__(self, queue_name: str, storage: Any, config: dict = {}):
        self.queue_name = queue_name
        self.storage = storage
        self.is_processing = False
        self.polling_interval = config.get('polling_interval', 5)  # Default to 5 seconds
        self.processor_fn = None
        self.polling_task = None

    async def initialize(self):
        await self.storage.initialize()
        await self.storage.ensure_metadata_exists(self.queue_name)

    async def add(self, job_data: dict) -> str:
        job_id = await self.storage.add_job(self.queue_name, job_data)
        # In Python, we don't have built-in event emitters, so we'll skip the 'added' event
        return job_id

    def process(self, processor_fn: Callable):
        self.processor_fn = processor_fn
        self.start_processing()

    def start_processing(self):
        if self.polling_task:
            self.polling_task.cancel()
        self.polling_task = asyncio.create_task(self.process_next_job())

    async def process_next_job(self):
        while True:
            if self.is_processing or not self.processor_fn:
                await asyncio.sleep(self.polling_interval)
                continue

            self.is_processing = True

            try:
                job = await self.storage.get_next_job(self.queue_name)
                if job:
                    # We'll skip the 'active' event here
                    try:
                        result = await self.processor_fn(job)
                        job['data']['result'] = result
                        job['data']['status'] = 'completed'
                        await self.storage.update_job(self.queue_name, job['id'], job['data'])
                        # Skip the 'completed' event
                    except Exception as error:
                        job['data']['status'] = 'failed'
                        job['data']['error'] = str(error)
                        await self.storage.update_job(self.queue_name, job['id'], job['data'])
                        # Skip the 'failed' event
            except Exception as error:
                print(f"Error processing job: {error}")
            finally:
                self.is_processing = False
                await asyncio.sleep(self.polling_interval)

    def stop(self):
        if self.polling_task:
            self.polling_task.cancel()
            self.polling_task = None