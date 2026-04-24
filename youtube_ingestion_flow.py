from prefect import flow, task
from prefect.logging import get_run_logger

import os
import requests
import pandas as pd
import time
from datetime import datetime
from pathlib import Path
import base64

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = "ilyasbejja"
GITHUB_REPO = "youtube_keywords_scrapper"
GITHUB_BRANCH = "main"

if not GITHUB_TOKEN:
    raise ValueError("Missing GITHUB_TOKEN environment variable")

# Configuration
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    raise ValueError("Missing YOUTUBE_API_KEY environment variable")

BASE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
BASE_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
BASE_COMMENT_THREADS_URL = "https://www.googleapis.com/youtube/v3/commentThreads"
BASE_COMMENTS_URL = "https://www.googleapis.com/youtube/v3/comments"

KEYWORDS_FILE = Path("keywords.csv")

OUTPUT_DIR = Path("data")
RAW_DIR = OUTPUT_DIR / "raw"
PROCESSED_DIR = OUTPUT_DIR / "processed"

VIDEOS_OUTPUT = PROCESSED_DIR / "videos.csv"
COMMENTS_OUTPUT = PROCESSED_DIR / "comments.csv"

MAX_VIDEOS_PER_KEYWORD = 5
MAX_COMMENT_PAGES_PER_VIDEO = 1

REQUEST_TIMEOUT = 30
REQUEST_SLEEP_SECONDS = 0.2

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


@task(name="load-keywords")
def load_keywords(csv_file: Path) -> list[str]:
    logger = get_run_logger()

    df = pd.read_csv(csv_file, usecols=["keyword"])

    keywords = (
        df["keyword"]
        .dropna()
        .astype(str)
        .str.strip()
    )

    keywords = keywords[keywords != ""].drop_duplicates().tolist()

    logger.info(f"Loaded {len(keywords)} unique keywords from {csv_file}")

    return keywords


@task(
    name="search-videos-for-keyword",
    retries=2,
    retry_delay_seconds=5
)
def search_videos_for_keyword(keyword: str, max_videos: int) -> list[dict]:
    logger = get_run_logger()

    all_videos = []
    next_page_token = None

    while len(all_videos) < max_videos:
        batch_size = min(50, max_videos - len(all_videos))

        params = {
            "part": "snippet",
            "q": keyword,
            "type": "video",
            "maxResults": batch_size,
            "key": API_KEY
        }

        if next_page_token:
            params["pageToken"] = next_page_token

        response = requests.get(
            BASE_SEARCH_URL,
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            video_id = item.get("id", {}).get("videoId")

            if not video_id:
                continue

            all_videos.append({
                "keyword": keyword,
                "video_id": video_id,
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "channel_title": snippet.get("channelTitle"),
                "channel_id": snippet.get("channelId"),
                "published_at": snippet.get("publishedAt")
            })

        next_page_token = data.get("nextPageToken")

        if not next_page_token:
            break

        time.sleep(REQUEST_SLEEP_SECONDS)

    logger.info(f"Keyword '{keyword}' returned {len(all_videos)} videos")

    return all_videos[:max_videos]


@task(
    name="fetch-video-details",
    retries=2,
    retry_delay_seconds=5
)
def fetch_video_details(video_rows: list[dict]) -> list[dict]:
    logger = get_run_logger()

    if not video_rows:
        logger.info("No video rows received for enrichment")
        return video_rows

    video_ids = [row["video_id"] for row in video_rows if row.get("video_id")]
    details_map = {}

    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]

        params = {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(chunk),
            "key": API_KEY
        }

        response = requests.get(
            BASE_VIDEOS_URL,
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            content = item.get("contentDetails", {})
            stats = item.get("statistics", {})

            details_map[item["id"]] = {
                "description": snippet.get("description"),
                "duration": content.get("duration"),
                "view_count": stats.get("viewCount"),
                "like_count": stats.get("likeCount"),
                "comment_count": stats.get("commentCount")
            }

        time.sleep(REQUEST_SLEEP_SECONDS)

    enriched_rows = []
    for row in video_rows:
        video_id = row.get("video_id")
        extra = details_map.get(video_id, {})

        enriched_row = {
            **row,
            **extra,
            "scraped_at": datetime.utcnow().isoformat()
        }
        enriched_rows.append(enriched_row)

    logger.info(f"Enriched {len(enriched_rows)} videos with details")

    return enriched_rows


@task(
    name="fetch-comments-for-video",
    retries=2,
    retry_delay_seconds=5
)
def fetch_comments_for_video(
    video_id: str,
    keyword: str,
    max_pages: int = 1
) -> list[dict]:
    logger = get_run_logger()

    all_comments = []
    next_page_token = None
    page_count = 0

    while page_count < max_pages:
        params = {
            "part": "snippet,replies",
            "videoId": video_id,
            "maxResults": 100,
            "textFormat": "plainText",
            "key": API_KEY
        }

        if next_page_token:
            params["pageToken"] = next_page_token

        response = requests.get(
            BASE_COMMENT_THREADS_URL,
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            top_comment = item.get("snippet", {}).get("topLevelComment", {})
            top_snippet = top_comment.get("snippet", {})

            all_comments.append({
                "keyword": keyword,
                "video_id": video_id,
                "comment_id": top_comment.get("id"),
                "parent_id": None,
                "author": top_snippet.get("authorDisplayName"),
                "text": top_snippet.get("textDisplay"),
                "published_at": top_snippet.get("publishedAt"),
                "updated_at": top_snippet.get("updatedAt"),
                "like_count": top_snippet.get("likeCount"),
                "is_reply": False,
                "total_reply_count": item.get("snippet", {}).get("totalReplyCount", 0),
                "scraped_at": datetime.utcnow().isoformat()
            })

        next_page_token = data.get("nextPageToken")
        page_count += 1

        if not next_page_token:
            break

        time.sleep(REQUEST_SLEEP_SECONDS)

    logger.info(f"Fetched {len(all_comments)} top-level comments for video {video_id}")

    return all_comments

def upload_file_to_github(local_file: Path, repo_path: str, commit_message: str) -> None:
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{repo_path}"

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    content_bytes = local_file.read_bytes()
    encoded_content = base64.b64encode(content_bytes).decode("utf-8")

    get_response = requests.get(
        url,
        headers=headers,
        params={"ref": GITHUB_BRANCH},
        timeout=REQUEST_TIMEOUT
    )

    sha = None
    if get_response.status_code == 200:
        sha = get_response.json().get("sha")
    elif get_response.status_code != 404:
        get_response.raise_for_status()

    payload = {
        "message": commit_message,
        "content": encoded_content,
        "branch": GITHUB_BRANCH,
    }

    if sha:
        payload["sha"] = sha

    put_response = requests.put(
        url,
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT
    )
    put_response.raise_for_status()

@task(name="upload-csvs-to-github")
def upload_csvs_to_github():
    logger = get_run_logger()

    upload_file_to_github(VIDEOS_OUTPUT, "data/processed/videos.csv", "update videos.csv")
    upload_file_to_github(COMMENTS_OUTPUT, "data/processed/comments.csv", "update comments.csv")

    logger.info("Uploaded CSV files to GitHub")


@task(
    name="fetch-replies-for-comment",
    retries=2,
    retry_delay_seconds=5
)
def fetch_replies_for_comment(
    parent_comment_id: str,
    video_id: str,
    keyword: str
) -> list[dict]:
    logger = get_run_logger()

    all_replies = []
    next_page_token = None

    while True:
        params = {
            "part": "snippet",
            "parentId": parent_comment_id,
            "maxResults": 100,
            "textFormat": "plainText",
            "key": API_KEY
        }

        if next_page_token:
            params["pageToken"] = next_page_token

        response = requests.get(
            BASE_COMMENTS_URL,
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            snippet = item.get("snippet", {})

            all_replies.append({
                "keyword": keyword,
                "video_id": video_id,
                "comment_id": item.get("id"),
                "parent_id": snippet.get("parentId"),
                "author": snippet.get("authorDisplayName"),
                "text": snippet.get("textDisplay"),
                "published_at": snippet.get("publishedAt"),
                "updated_at": snippet.get("updatedAt"),
                "like_count": snippet.get("likeCount"),
                "is_reply": True,
                "total_reply_count": None,
                "scraped_at": datetime.utcnow().isoformat()
            })

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

        time.sleep(REQUEST_SLEEP_SECONDS)

    logger.info(f"Fetched {len(all_replies)} replies for parent comment {parent_comment_id}")

    return all_replies


@task(name="save-videos")
def save_videos(video_rows: list[dict], output_path: Path) -> None:
    logger = get_run_logger()

    df = pd.DataFrame(video_rows)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    logger.info(f"Saved {len(df)} video rows to {output_path}")


@task(name="save-comments")
def save_comments(comment_rows: list[dict], output_path: Path) -> None:
    logger = get_run_logger()

    df = pd.DataFrame(comment_rows)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    logger.info(f"Saved {len(df)} comment rows to {output_path}")


@flow(name="youtube-ingestion-flow")
def youtube_ingestion_flow():
    logger = get_run_logger()

    logger.info("Starting YouTube ingestion flow")

    keywords = load_keywords(KEYWORDS_FILE)

    all_videos = []
    all_comments = []

    for idx, keyword in enumerate(keywords, start=1):
        logger.info(f"[{idx}/{len(keywords)}] Processing keyword: {keyword}")

        video_rows = search_videos_for_keyword(
            keyword=keyword,
            max_videos=MAX_VIDEOS_PER_KEYWORD
        )

        video_rows = fetch_video_details(video_rows)
        all_videos.extend(video_rows)

        for video in video_rows:
            video_id = video["video_id"]

            try:
                top_comments = fetch_comments_for_video(
                    video_id=video_id,
                    keyword=keyword,
                    max_pages=MAX_COMMENT_PAGES_PER_VIDEO
                )

                all_comments.extend(top_comments)

                for comment in top_comments:
                    total_reply_count = comment.get("total_reply_count", 0)
                    parent_comment_id = comment.get("comment_id")

                    if total_reply_count and parent_comment_id:
                        replies = fetch_replies_for_comment(
                            parent_comment_id=parent_comment_id,
                            video_id=video_id,
                            keyword=keyword
                        )
                        all_comments.extend(replies)

            except requests.HTTPError as e:
                logger.warning(f"Skipping comments for video {video_id}: {e}")

            time.sleep(REQUEST_SLEEP_SECONDS)

    save_videos(all_videos, VIDEOS_OUTPUT)
    save_comments(all_comments, COMMENTS_OUTPUT)
    upload_csvs_to_github()

    logger.info("YouTube ingestion flow completed")


if __name__ == "__main__":
    youtube_ingestion_flow()
