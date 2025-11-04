import json
import time
import threading
from threading import Lock
from datetime import datetime, timezone
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
# vader = SentimentIntensityAnalyzer()
# transformer = pipeline('sentiment-analysis', 
#                       model="cardiffnlp/twitter-roberta-base-sentiment-latest")
print("Sentiment analyzers loaded!")

kafka_config = {
    'bootstrap.servers': kafka_creds['bootstrap_servers'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': kafka_creds['api_key'],
    'sasl.password': kafka_creds['api_secret'],
}

producer = Producer(kafka_config)
stats_lock = Lock()
total_posts = 0
total_comments = 0

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_message(key, value):
    producer.produce(
                    topic='reddit-sentiment',
                    key=str(key),
                    value=json.dumps(value),
                    callback=delivery_report
                )

def fetch_subreddit(subreddit_names, posts_per_subreddit=10):
    """Fetch Reddit posts from multiple subreddits and produce to Kafka"""
    
    total_produced = 0
    total_comms = 0
    
    for subreddit_name in subreddit_names:
        print(f"\n Fetching from r/{subreddit_name}")
        
        try:
            subreddit = reddit.subreddit(subreddit_name)
            
            for post in subreddit.new(limit=posts_per_subreddit):
                
                post_text = f"{post.title}. {post.selftext}" if post.selftext else post.title
                # post_com = f"{comment.body}" if comment.body else ""
                
                if not post_text or not post_text.strip():
                    continue
                post_message = {
                    'type': 'post',
                    'post_id': post.id,
                    'subreddit': subreddit_name,
                    'title': post.title,
                    'selftext': post.selftext,
                    'post_text': post_text,
                    'author': str(post.author) if post.author else 'None',
                    'created_utc': post.created_utc,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'score': post.score,
                    'num_comments': post.num_comments,
                    'upvote_ratio': post.upvote_ratio,
                    'permalink': post.permalink,
                    'url': post.url
                }
                produce_message(post.id, post_message)
                total_produced += 1
               
               
                post.comment_sort = "top"   
                post.comments.replace_more(limit=0)

                for comment in post.comments[:15]:
                    comment_message = {
                        'type': 'comment',
                        'comment_id': comment.id,
                        'post_id': post.id,
                        'subreddit': subreddit_name,
                        'body': comment.body,
                        'author': str(post.comment) if post.author else 'None',
                        'created_utc': comment.created_utc,
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'score': comment.score,
                        'permalink': comment.permalink
                    }
                    produce_message(comment.id, comment_message)
                    total_comms += 1
                
                producer.poll(0)

        except Exception as e:
            print(f"Error fetching from r/{subreddit_name}: {e}")
    
    print("\nFlushing remaining messages...")
    producer.flush()
    print(f" Total posts produced: {total_produced}")
    print(f" Total comments produced: {total_comms}")
    return total_produced, total_comms

def fetch_and_produce_threaded(subreddit_names, posts_per_subreddit=10, comments_per_post=15):
    """Fetch Reddit posts using threading for multiple subreddits"""
    global total_posts, total_comments

    total_posts = 0
    total_comments = 0
    threads = []
    for subreddit_name in subreddit_names:
        thread = threading.Thread(
            target=fetch_subreddit,
            args=(subreddit_name, posts_per_subreddit, comments_per_post)
        )
        threads.append(thread)
        thread.start()  
    
    for thread in threads:
        thread.join()
    
    # Flush all messages
    print("\nFlushing remaining messages...")
    producer.flush()
    
    print(f"\n Total posts produced: {total_posts}")
    print(f"Total comments produced: {total_comments}")
    print(f" Grand total: {total_posts + total_comments}")
    
    return total_posts, total_comments


if __name__ == "__main__":
    print("="*60)
    print("Reddit Producer with Threading (Posts + Comments)")
    print("="*60)
    
    # List of subreddits
    subreddits = [
        'technology', 'python', 'datascience', 'MachineLearning',
        'programming', 'artificial', 'news', 'worldnews'
    ]
    
    print(f"\nTarget subreddits ({len(subreddits)}): {', '.join(subreddits)}")
    print(f"Posts per subreddit: 10")
    print(f"Comments per post: 15")
    print(f"Threads: {len(subreddits)} (one per subreddit)")
    
    start_time = time.time()
    
    print("\n🚀 Starting threaded fetch...\n")
    fetch_and_produce_threaded(subreddits, posts_per_subreddit=10, comments_per_post=15)
    
    elapsed = time.time() - start_time
    print(f"\nTotal time: {elapsed:.2f} seconds")
    print("✅ Producer finished!")



# ADD THREADING TO RUN MULTIPLE FETCHES IN PARALLEL IF NEEDED