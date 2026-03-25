import requests
import pandas as pd
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

# Config
SUBREDDITS = ['datascience', 'MachineLearning', 'technology', 'worldnews', 'stocks']
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')
RAW_DATA_PATH = 'raw/reddit/posts'

def fetch_subreddit_posts(subreddit, limit=100, time_filter='day'):
    """Fetch posts from a subreddit using public JSON API"""
    url = f"https://www.reddit.com/r/{subreddit}/top.json"
    headers = {'User-Agent': 'Mozilla/5.0'}
    params = {'limit': limit, 't': time_filter}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        posts = []
        for post in data['data']['children']:
            p = post['data']
            posts.append({
                'id': p['id'],
                'title': p['title'],
                'score': p['score'],
                'upvote_ratio': p['upvote_ratio'],
                'num_comments': p['num_comments'],
                'created_utc': p['created_utc'],
                'created_date': datetime.fromtimestamp(p['created_utc']).strftime('%Y-%m-%d'),
                'created_hour': datetime.fromtimestamp(p['created_utc']).hour,
                'subreddit': p['subreddit'],
                'author': p['author'],
                'url': p['url'],
                'is_self': p['is_self'],
                'selftext': p['selftext'][:500] if p['selftext'] else '',
                'permalink': f"https://reddit.com{p['permalink']}",
                'flair': p.get('link_flair_text', ''),
                'ingested_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        print(f"✅ Fetched {len(posts)} posts from r/{subreddit}")
        return pd.DataFrame(posts)
    
    except Exception as e:
        print(f"❌ Error fetching r/{subreddit}: {e}")
        return pd.DataFrame()

def save_to_gcs(df, subreddit):
    """Save DataFrame as parquet to GCS"""
    if df.empty:
        print(f"⚠️ No data to save for r/{subreddit}")
        return
    
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    
    # Save as parquet
    date_str = datetime.now().strftime('%Y-%m-%d')
    filename = f"{RAW_DATA_PATH}/{subreddit}/{date_str}.parquet"
    
    # Convert to parquet bytes
    parquet_data = df.to_parquet(index=False)
    
    # Upload to GCS
    blob = bucket.blob(filename)
    blob.upload_from_string(parquet_data, content_type='application/octet-stream')
    
    print(f"✅ Saved {len(df)} posts to gs://{GCS_BUCKET}/{filename}")

def run_ingestion():
    """Main ingestion function"""
    print(f"🚀 Starting Reddit ingestion at {datetime.now()}")
    print(f"📋 Subreddits: {SUBREDDITS}\n")
    
    all_posts = []
    
    for subreddit in SUBREDDITS:
        # Fetch posts
        df = fetch_subreddit_posts(subreddit, limit=100, time_filter='day')
        
        if not df.empty:
            # Save to GCS
            save_to_gcs(df, subreddit)
            all_posts.append(df)
    
    # Combine all posts
    if all_posts:
        combined_df = pd.concat(all_posts, ignore_index=True)
        print(f"\n📊 Total posts fetched: {len(combined_df)}")
        print(f"📊 Subreddits: {combined_df['subreddit'].value_counts().to_dict()}")
        return combined_df
    
    return pd.DataFrame()

if __name__ == '__main__':
    df = run_ingestion()
    print("\n✅ Ingestion complete!")
    print(df[['subreddit', 'title', 'score', 'num_comments']].head(10))