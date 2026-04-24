from youtube_ingestion_flow import youtube_ingestion_flow

if __name__ == "__main__":
    youtube_ingestion_flow.deploy(
        name="youtube-ingestion-deployment",
        work_pool_name="my-process-pool",
        cron="0 2 * * *",
        push=False
    )