import json
import time
from datetime import datetime
from confluent_kafka import Producer
import praw
from nltk.sentiment import SentimentIntensityAnalyzer
from transformers import pipeline
import nltk

# ADD THREADING TO RUN MULTIPLE FETCHES IN PARALLEL IF NEEDED

creds = {}
with open(r'PROJECT\.venv\reddit_credentials.txt', 'r') as f:
    for line in f:
        if '=' in line:
            key, value = line.strip().split("=", 1)
            creds[key] = value

kafka_creds = {}
with open(r'PROJECT\.venv\kafka_credentials.txt', 'r') as f:
    for line in f:
        if '=' in line:
            key, value = line.strip().split("=", 1)
            kafka_creds[key] = value

reddit = praw.Reddit(client_id=creds['client_id'],
                     client_secret=creds['client_secret'],
                     user_agent=creds['user_agent'])

print("Loading sentiment analyzers...")
vader = SentimentIntensityAnalyzer()
transformer = pipeline('sentiment-analysis', 
                      model="cardiffnlp/twitter-roberta-base-sentiment-latest")
print("Sentiment analyzers loaded!")

kafka_config = {
    'bootstrap.servers': kafka_creds['bootstrap_servers'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': kafka_creds['api_key'],
    'sasl.password': kafka_creds['api_secret'],
}

producer = Producer(kafka_config)

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✓ Message delivered to {msg.topic()} [{msg.partition()}]')

def fetch_and_produce(subreddit_names, posts_per_subreddit=10):
    """Fetch Reddit posts from multiple subreddits and produce to Kafka"""
    
    total_produced = 0
    
    for subreddit_name in subreddit_names:
        print(f"\n📡 Fetching from r/{subreddit_name}...")
        
        try:
            subreddit = reddit.subreddit(subreddit_name)
            subreddit_coms = subreddit.comments(limit=10)
            
            # Fetch hot posts from the subreddit
            for post, comment in zip(subreddit_coms, subreddit.new(limit=posts_per_subreddit)):
                
                post_text = f"{post.title}. {post.selftext}" if post.selftext else post.title
                post_com = f"{comment.body}" if comment.body else ""
                
                if not post_text or not post_text.strip():
                    continue
                message = {
                    'post_id': post.id,
                    'subreddit': subreddit_name,
                    'title': post.title,
                    'selftext': post.selftext,
                    'post_text': post_text,
                    'author': str(post.author),
                    'created_utc': post.created_utc,
                    'score': post.score,
                    'num_comments': post.num_comments,
                    'upvote_ratio': post.upvote_ratio,
                    'permalink': post.permalink,
                    'url': post.url
                }

                message2 = {
                    'comment_id': comment.id,
                    'post_id': post.id,
                    'subreddit': subreddit_name,
                    'comment_body': post_com,
                    'author': str(comment.author),
                    'created_utc': comment.created_utc,
                    'score': comment.score,
                    'permalink': comment.permalink
                }
                
                producer.produce(
                    'reddit-sentiment',
                    key=str(post.id),
                    value=json.dumps(message),
                    value=json.dumps(message2),
                    callback=delivery_report
                )
                
                total_produced += 1
                
                producer.poll(0)

        except Exception as e:
            print(f"❌ Error fetching from r/{subreddit_name}: {e}")
    
    print("\n⏳ Flushing remaining messages...")
    producer.flush()
    print(f"✅ Total posts produced: {total_produced}")
    return total_produced


if __name__ == "__main__":
    print("="*60)
    print("Reddit Post Producer (No Sentiment Analysis)")
    print("="*60)
    

    subreddits = [
        'technology'
    ]

print(f"\nTarget subreddits ({len(subreddits)}): {', '.join(subreddits[:5])}...")
print(f"Posts per subreddit: 10")
print(f"Total expected posts: ~{len(subreddits) * 10}")

print("\n🚀 Starting single fetch...")
fetch_and_produce(subreddits, posts_per_subreddit=10)
print("\n✅ Producer finished!")



# ADD THREADING TO RUN MULTIPLE FETCHES IN PARALLEL IF NEEDED