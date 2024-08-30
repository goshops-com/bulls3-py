import asyncio
from bull_s3_python import BullS3, AzureBlobStorage

# Azure Blob Storage configuration
azure_config = {
    'connection_string': 'YOUR_AZURE_CONNECTION_STRING',
    'container_name': 'YOUR_CONTAINER_NAME'
}

# Initialize Azure Blob Storage
storage = AzureBlobStorage(azure_config)

# Initialize BullS3 with Azure Blob Storage
queue = BullS3('test-queue', storage)

async def example_job_processor(job):
    print(f"Processing job: {job['id']}")
    # Simulate some work
    await asyncio.sleep(2)
    print(f"Job {job['id']} completed")
    return f"Result of job {job['id']}"

async def main():
    # Initialize the queue
    await queue.initialize()

    # Add some jobs to the queue
    for i in range(5):
        job_id = await queue.add({'task': f'Task {i}'})
        print(f"Added job {job_id}")

    # Start processing jobs
    queue.process(example_job_processor)

    # Keep the script running to process jobs
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Stopping the queue")
        queue.stop()

if __name__ == "__main__":
    asyncio.run(main())