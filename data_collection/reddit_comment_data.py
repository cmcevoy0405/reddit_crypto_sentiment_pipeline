# Import libraries
import time
import httpx
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

project_id = os.getenv('PROJECT_ID')
dataset_id = os.getenv('DATASET_ID')
cred_path= os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

print("Using credentials:", cred_path)

client = bigquery.Client(project=project_id)

# safe scraping logic
def get_safe_url(url, params, retries = 5):
    for attempt in range(retries):
        try:
            r = httpx.get(url, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                return r
            print(f"Status {r.status_code}, retrying...")
        except Exception:
            print("Connection failed, retrying...")

        time.sleep(2 ** attempt)
    
    raise Exception("Tried too many times")

# url building for posts dataset
base_url = 'https://www.reddit.com'
end_point = '/r/CryptoCurrency/new'

url = base_url + end_point + ".json"
after_post_id = None

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

all_posts = []
after_post_id = None

one_hour_ago = datetime.utcnow() - timedelta(hours=24)

while True:
    params = {
        'limit': 100,
        'after': after_post_id
    }
    response = get_safe_url(url, params)
    data = response.json()

    children = data['data']['children']
    if not children:
        break

    # Filter posts created in the last hour
    for rec in children:
        post_time = datetime.utcfromtimestamp(rec['data']['created_utc'])
        if post_time >= one_hour_ago:
            all_posts.append(rec['data'])
        else:
            # Stop scraping if we hit older posts
            print("Reached posts older than 1 day, stopping...")
            break
    else:
        # Update after_post_id for next page if all posts were recent
        after_post_id = data['data']['after']
        if not after_post_id:
            break
        time.sleep(2)
        continue
    break

print(f"Total posts in the last day: {len(all_posts)}")

# Url building for comments on subreddit
url = 'https://www.reddit.com'
comment_point = '/r/CryptoCurrency/comments'

comment_url = url + comment_point + ".json"
after_post_id = None

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

all_comments = []
after_comment_id = None

one_hour_ago = datetime.utcnow() - timedelta(hours=24)

while True:
    params = {
        'limit': 100,
        'after': after_comment_id
    }
    response = get_safe_url(comment_url, params)
    data = response.json()

    children = data['data']['children']
    if not children:
        break

    # Filter posts created in the last hour
    for rec in children:
        post_time = datetime.utcfromtimestamp(rec['data']['created_utc'])
        if post_time >= one_hour_ago:
            all_comments.append(rec['data'])
        else:
            # Stop scraping if we hit older posts
            print("Reached comments older than 1 hour, stopping...")
            break
    else:
        # Update after_post_id for next page if all posts were recent
        after_comment_id = data['data']['after']
        if not after_comment_id:
            break
        time.sleep(2)
        continue
    break

print(f"Total comments in the last day: {len(all_comments)}")

# Post and comment dataframwe
post_df = pd.DataFrame(all_posts)
comment_df = pd.DataFrame(all_comments)

for df in [post_df, comment_df]:
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)

# Tables for big query
post_table_id = f"{project_id}.{dataset_id}.raw_reddit_posts"
comment_table_id = f"{project_id}.{dataset_id}.raw_reddit_comments"

post_job = client.load_table_from_dataframe(
    post_df,
    post_table_id,
    job_config=bigquery.LoadJobConfig(write_disposition = 'WRITE_TRUNCATE')
)
post_job.result()
print(f"Uploaded {len(post_df)} posts to {post_table_id}")

comment_job = client.load_table_from_dataframe(
    comment_df,
    comment_table_id,
    job_config=bigquery.LoadJobConfig(write_disposition = 'WRITE_TRUNCATE')
)
comment_job.result()
print(f"Uploaded {len(comment_df)} posts to {comment_table_id}")