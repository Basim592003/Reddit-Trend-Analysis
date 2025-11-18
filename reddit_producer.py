import json
import time
import threading
from threading import Lock
from datetime import datetime, timezone
from confluent_kafka import Producer
import praw
import yaml
from prawcore.exceptions import TooManyRequests, RequestException

creds = {}
with open(r'reddit_credentials.txt', 'r') as f:
    for line in f:
        if '=' in line:
            key, value = line.strip().split("=", 1)
            creds[key] = value

kafka_creds = {}
with open(r'kafka_credentials.txt', 'r') as f:
    for line in f:
        if '=' in line:
            key, value = line.strip().split("=", 1)
            kafka_creds[key] = value

reddit = praw.Reddit(client_id=creds['client_id'],
                     client_secret=creds['client_secret'],
                     user_agent=creds['user_agent'])

print(" Reddit client initialized!")

kafka_config = {
    'bootstrap.servers': kafka_creds['bootstrap_servers'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': kafka_creds['api_key'],
    'sasl.password': kafka_creds['api_secret'],
}

producer = Producer(kafka_config)
print(" Kafka producer initialized!")

stats_lock = Lock()
rate_limit_lock = Lock()

def delivery_report(err, msg):
    if err is not None:
        print(f' Delivery failed: {err}')

def produce_message(key, value):
    producer.produce(
        topic='reddit-sentiment',
        key=str(key),
        value=json.dumps(value),
        callback=delivery_report
    )

def fetch_subreddit(subreddit_name, posts_per_subreddit, comments_per_post, stats_dict):
    local_posts = 0
    local_comments = 0
    
    print(f"[{subreddit_name}] Starting fetch...")
    
    try:
        subreddit = reddit.subreddit(subreddit_name)
        
        for post in subreddit.new(limit=posts_per_subreddit):
            with rate_limit_lock:
                time.sleep(1)
            
            post_text = f"{post.title}. {post.selftext}" if post.selftext else post.title
            
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
                'url': post.url,
                'permalink': f"https://www.reddit.com{post.permalink}"
                
            }
            produce_message(post.id, post_message)
            local_posts += 1
            
            try:
                post.comment_sort = "top"   
                post.comments.replace_more(limit=0)

                for comment in post.comments[:comments_per_post]:
                    if not comment.body or comment.body in ['[deleted]', '[removed]']:
                        continue
                        
                    comment_message = {
                        'type': 'comment',
                        'comment_id': comment.id,
                        'post_id': post.id,
                        'subreddit': subreddit_name,
                        'body': comment.body,
                        'author': str(comment.author) if comment.author else 'None',
                        'created_utc': comment.created_utc,
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'score': comment.score,
                        'url': f"https://www.reddit.com{comment.permalink}"
                    }
                    produce_message(comment.id, comment_message)
                    local_comments += 1
                    
            except TooManyRequests:
                print(f"    [{subreddit_name}] Rate limit hit! Waiting 60s...")
                time.sleep(60)
            except Exception as e:
                print(f"    [{subreddit_name}] Error fetching comments: {e}")
            
            producer.poll(0)

    except TooManyRequests:
        print(f"[{subreddit_name}] Rate limit hit! Waiting 60s...")
        time.sleep(60)
    except Exception as e:
        print(f"[{subreddit_name}] Error: {e}")
    
    with stats_lock:
        stats_dict['total_posts'] += local_posts
        stats_dict['total_comments'] += local_comments
    
    print(f" [{subreddit_name}], Posts: {local_posts}, Comments: {local_comments}")

def fetch_and_produce_threaded(subreddit_names, posts_per_subreddit, comments_per_post):
    if isinstance(subreddit_names, str):
        raise TypeError(f"subreddit_names not a list")
    
    stats_dict = {
        'total_posts': 0,
        'total_comments': 0
    }
    
    threads = []
    
    for subreddit_name in subreddit_names:
        thread = threading.Thread(
            target=fetch_subreddit,
            args=(subreddit_name, posts_per_subreddit, comments_per_post, stats_dict)
        )
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    producer.flush()
    
    return stats_dict['total_posts'], stats_dict['total_comments']


if __name__ == "__main__":
    print("="*60)
    print("Reddit Producer with Threading (Posts + Comments)")
    print("="*60)
    

    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    subreddits = config['subreddits']
    POSTS_LIMIT = config['posts_per_subreddit']
    COMMENTS_LIMIT = config['comments_per_post']

    start_time = time.time()
    
    total_posts, total_comments = fetch_and_produce_threaded(subreddits, posts_per_subreddit=POSTS_LIMIT, comments_per_post=COMMENTS_LIMIT)
    
    elapsed = time.time() - start_time
    
    print(f"\n Total time: {elapsed:.2f} seconds")
    print(f" Total posts produced: {total_posts}")
    print(f" Total comments produced: {total_comments}")
    print(f" Grand total: {total_posts + total_comments}")
    print(" Producer finished!")
    
    if total_posts == len(subreddits) * POSTS_LIMIT:
        print(f" SUCCESS: Got all {total_posts} expected posts!")
    else:
        print(f" WARNING: Expected {len(subreddits) * POSTS_LIMIT} posts, got {total_posts}")