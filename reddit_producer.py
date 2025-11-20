import json
import time
import threading
from threading import Lock
from datetime import datetime, timezone
from confluent_kafka import Producer
import praw
import yaml
from prawcore.exceptions import TooManyRequests, RequestException
import logging
from logging.handlers import RotatingFileHandler
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('producer.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT')
)

logger.info("Reddit client initialized!")

kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}

producer = Producer(kafka_config)
logger.info("Kafka producer initialized!")

stats_lock = Lock()
rate_limit_lock = Lock()

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Delivery failed: {err}')

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
    
    logger.info(f"[{subreddit_name}] Starting fetch...")
    
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
                logger.warning(f"[{subreddit_name}] Rate limit hit! Waiting 60s...")
                time.sleep(60)
            except Exception as e:
                logger.error(f"[{subreddit_name}] Error fetching comments: {e}")
            
            producer.poll(0)

    except TooManyRequests:
        logger.warning(f"[{subreddit_name}] Rate limit hit! Waiting 60s...")
        time.sleep(60)
    except Exception as e:
        logger.error(f"[{subreddit_name}] Error: {e}")
    
    with stats_lock:
        stats_dict['total_posts'] += local_posts
        stats_dict['total_comments'] += local_comments
    
    logger.info(f"[{subreddit_name}] Complete - Posts: {local_posts}, Comments: {local_comments}")

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
    logger.info("="*60)
    logger.info("Reddit Producer - Continuous Mode")
    logger.info("="*60)
    
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    ALL_SUBREDDITS = config['subreddits']
    BATCH_SIZE = config['batch_size']
    POSTS_PER_SUBREDDIT = config['posts_per_subreddit']
    COMMENTS_PER_POST = config['comments_per_post']
    CYCLE_DELAY = config['cycle_delay']

    
    batch_number = 0
    
    while True:
        try:
            start_idx = (batch_number * BATCH_SIZE) % len(ALL_SUBREDDITS)
            end_idx = start_idx + BATCH_SIZE
            
            if end_idx > len(ALL_SUBREDDITS):
                current_batch = ALL_SUBREDDITS[start_idx:] + ALL_SUBREDDITS[:end_idx - len(ALL_SUBREDDITS)]
            else:
                current_batch = ALL_SUBREDDITS[start_idx:end_idx]
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Batch #{batch_number + 1} - Processing: {', '.join(current_batch)}")
            logger.info(f"{'='*60}")
            
            start_time = time.time()
            
            total_posts, total_comments = fetch_and_produce_threaded(
                current_batch, 
                posts_per_subreddit=POSTS_PER_SUBREDDIT, 
                comments_per_post=COMMENTS_PER_POST
            )
            
            elapsed = time.time() - start_time
            
            logger.info(f"\nBatch #{batch_number + 1} Complete:")
            logger.info(f"  Time: {elapsed:.2f}s")
            logger.info(f"  Posts: {total_posts}")
            logger.info(f"  Comments: {total_comments}")
            logger.info(f"  Total: {total_posts + total_comments}")
            
            batch_number += 1
            
            if batch_number * BATCH_SIZE >= len(ALL_SUBREDDITS):
                logger.info(f"\nCompleted full cycle of all {len(ALL_SUBREDDITS)} subreddits")
                logger.info(f"Waiting {CYCLE_DELAY}s before starting next cycle...")
                time.sleep(CYCLE_DELAY)
                batch_number = 0
            else:
                logger.info(f"\nWaiting 60s before next batch...")
                time.sleep(60)
                
        except KeyboardInterrupt:
            logger.info("\nShutdown requested by user")
            logger.info(f"  Posts: {total_posts}")
            logger.info(f"  Comments: {total_comments}")
            producer.flush()
            break
        except Exception as e:
            logger.error(f"Critical error in main loop: {e}", exc_info=True)
            logger.info("Waiting 300s before retry...")
            time.sleep(300)