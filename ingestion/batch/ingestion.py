import requests
import pandas as pd
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

# Config
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')
RAW_DATA_PATH = 'raw/hackernews/posts'
BASE_URL = "https://hacker-news.firebaseio.com/v0"

def fetch_story(story_id):
    """Fetch a single story by ID"""
    try:
        url = f"{BASE_URL}/item/{story_id}.json"
        response = requests.get(url)
        return response.json()
    except Exception as e:
        print(f"❌ Error fetching story {story_id}: {e}")
        return None

def fetch_top_stories(limit=100):
    """Fetch top stories from Hacker News"""
    print(f"🔍 Fetching top {limit} stories from Hacker News...")
    
    # Get top story IDs
    url = f"{BASE_URL}/topstories.json"
    response = requests.get(url)
    story_ids = response.json()[:limit]
    
    posts = []
    for i, story_id in enumerate(story_ids):
        story = fetch_story(story_id)
        
        if story and story.get('type') == 'story':
            posts.append({
                'id': str(story.get('id')),
                'title': story.get('title', ''),
                'score': story.get('score', 0),
                'num_comments': story.get('descendants', 0),
                'created_utc': story.get('time', 0),
                'created_date': datetime.fromtimestamp(
                    story.get('time', 0)
                ).strftime('%Y-%m-%d'),
                'created_hour': datetime.fromtimestamp(
                    story.get('time', 0)
                ).hour,
                'author': story.get('by', ''),
                'url': story.get('url', ''),
                'source': 'hackernews',
                'ingested_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Progress update every 10 stories
        if (i + 1) % 10 == 0:
            print(f"  ⏳ Fetched {i + 1}/{limit} stories...")
        
        # Be respectful — small delay
        time.sleep(0.05)
    
    print(f"✅ Fetched {len(posts)} stories from Hacker News")
    return pd.DataFrame(posts)

def fetch_best_stories(limit=100):
    """Fetch best stories from Hacker News"""
    print(f"🔍 Fetching best {limit} stories from Hacker News...")
    
    url = f"{BASE_URL}/beststories.json"
    response = requests.get(url)
    story_ids = response.json()[:limit]
    
    posts = []
    for i, story_id in enumerate(story_ids):
        story = fetch_story(story_id)
        
        if story and story.get('type') == 'story':
            posts.append({
                'id': str(story.get('id')),
                'title': story.get('title', ''),
                'score': story.get('score', 0),
                'num_comments': story.get('descendants', 0),
                'created_utc': story.get('time', 0),
                'created_date': datetime.fromtimestamp(
                    story.get('time', 0)
                ).strftime('%Y-%m-%d'),
                'created_hour': datetime.fromtimestamp(
                    story.get('time', 0)
                ).hour,
                'author': story.get('by', ''),
                'url': story.get('url', ''),
                'source': 'hackernews_best',
                'ingested_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        if (i + 1) % 10 == 0:
            print(f"  ⏳ Fetched {i + 1}/{limit} stories...")
        
        time.sleep(0.05)
    
    print(f"✅ Fetched {len(posts)} best stories from Hacker News")
    return pd.DataFrame(posts)

def save_to_gcs(df, source):
    """Save DataFrame as parquet to GCS"""
    if df.empty:
        print(f"⚠️ No data to save for {source}")
        return
    
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    filename = f"{RAW_DATA_PATH}/{source}/{date_str}.parquet"
    
    parquet_data = df.to_parquet(index=False)
    blob = bucket.blob(filename)
    blob.upload_from_string(
        parquet_data, 
        content_type='application/octet-stream'
    )
    
    print(f"✅ Saved {len(df)} posts to gs://{GCS_BUCKET}/{filename}")

def run_ingestion():
    """Main ingestion function"""
    print(f"🚀 Starting Hacker News ingestion at {datetime.now()}")
    
    all_posts = []
    
    # Fetch top stories
    top_df = fetch_top_stories(limit=100)
    if not top_df.empty:
        save_to_gcs(top_df, 'top')
        all_posts.append(top_df)
    
    # Fetch best stories
    best_df = fetch_best_stories(limit=100)
    if not best_df.empty:
        save_to_gcs(best_df, 'best')
        all_posts.append(best_df)
    
    # Combine all
    if all_posts:
        combined_df = pd.concat(all_posts, ignore_index=True)
        print(f"\n📊 Total stories fetched: {len(combined_df)}")
        print(f"📊 Sources: {combined_df['source'].value_counts().to_dict()}")
        print(f"📊 Date range: {combined_df['created_date'].min()} to {combined_df['created_date'].max()}")
        print("\n🔝 Top 5 stories by score:")
        print(combined_df.nlargest(5, 'score')[['title', 'score', 'num_comments']].to_string())
        return combined_df
    
    return pd.DataFrame()

if __name__ == '__main__':
    df = run_ingestion()
    print("\n✅ Ingestion complete!")