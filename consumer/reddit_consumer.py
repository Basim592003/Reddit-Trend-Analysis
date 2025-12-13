import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import os
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('consumer.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

try:
    vader = SentimentIntensityAnalyzer()
except:
    nltk.download('vader_lexicon')
    vader = SentimentIntensityAnalyzer()

kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'group.id': 'reddit-sentiment-consumer',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_config)

try:
    mongo_client = MongoClient(os.getenv('MONGO_CONNECTION_STRING'))
    db = mongo_client['reddit_sentiment']
    post = db['posts']
    comment = db['comments']
except Exception as e:
    logger.error(f"Error connecting to MongoDB: {e}")

post.create_index('post_id', unique=True)
post.create_index('subreddit')
post.create_index('score')
post.create_index('timestamp', expireAfterSeconds=int(604800/2.5)) 

comment.create_index('comment_id', unique=True)
comment.create_index('post_id')
comment.create_index('subreddit')
comment.create_index('score')
comment.create_index('timestamp', expireAfterSeconds=int(604800/2.5))

new_posts_added = 0
new_comments_added = 0
updated_posts = 0
updated_comments = 0
error_posts = 0
error_comments = 0


def analyze_sentiment(text):
    if not text or not text.strip():
        return None

    try:
        vader_score = vader.polarity_scores(text)['compound']
        
        if vader_score >= 0.05:
            sentiment_label = 'positive'
        elif vader_score <= -0.05:
            sentiment_label = 'negative'
        else:
            sentiment_label = 'neutral'
        
        return {
            'vader_score': vader_score,
            'sentiment_label': sentiment_label,
            'sentiment_confidence': abs(vader_score)
        }
    except Exception as e:
        logger.error(f"Error analyzing sentiment: {e}")
        return None


def process_message(message):
    global new_posts_added, new_comments_added
    global updated_posts, updated_comments
    global error_posts, error_comments

    try:
        data = json.loads(message.value().decode('utf-8'))
        msg_type = data.get('type')

        if msg_type == 'post':
            text_to_analyze = data.get('post_text') or data.get('title', '')
        else:
            text_to_analyze = data.get('body', '')

        sentiment = analyze_sentiment(text_to_analyze)

        if sentiment is None:
            identifier = data.get('post_id') or data.get('comment_id')
            logger.info(f"SKIPPED (No sentiment): {identifier}")

            if msg_type == 'post':
                error_posts += 1
            else:
                error_comments += 1

            return False

        data.update({
            'vader_score': sentiment['vader_score'],
            'transformer_label': sentiment['sentiment_label'],
            'transformer_confidence': sentiment['sentiment_confidence'],
            'processed_at': datetime.now(timezone.utc).isoformat()
        })

        if msg_type == 'post':
            result = post.update_one(
                {'post_id': data['post_id']},
                {'$set': data},
                upsert=True
            )

            if result.upserted_id:
                logger.info(f"NEW POST: {data['post_id']} | r/{data['subreddit']}")
                new_posts_added += 1
            else:
                logger.info(f"UPDATED POST: {data['post_id']}")
                updated_posts += 1

        else:  
            result = comment.update_one(
                {'comment_id': data['comment_id']},
                {'$set': data},
                upsert=True
            )

            if result.upserted_id:
                logger.info(f"NEW COMMENT: {data['comment_id']} | r/{data['subreddit']}")   
                new_comments_added += 1
            else:
                logger.info(f"UPDATED COMMENT: {data['comment_id']}")
                updated_comments += 1

        return True

    except Exception as e:
        try:
            raw = json.loads(message.value().decode('utf-8'))
            identifier = raw.get('post_id') or raw.get('comment_id')
            msg_type = raw.get('type')
        except:
            identifier = None
            msg_type = None

        logger.error(f"ERROR processing item {identifier}: {e}")

        if msg_type == 'post':
            error_posts += 1
        else:
            error_comments += 1

        return False


def consume_messages():
    consumer.subscribe(['reddit-sentiment'])
    logger.info("Consumer started - waiting for messages...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

            process_message(msg)

    except KeyboardInterrupt:
        logger.info("Shutdown requested...")

    finally:
        consumer.close()
        mongo_client.close()

        logger.info("\n=============================")
        logger.info("       MONGO SUMMARY")
        logger.info("=============================")
        logger.info(f"New posts added:       {new_posts_added}")
        logger.info(f"Updated posts:         {updated_posts}")
        logger.info(f"Post errors:           {error_posts}")
        logger.info("")
        logger.info(f"New comments added:    {new_comments_added}")
        logger.info(f"Updated comments:      {updated_comments}")
        logger.info(f"Comment errors:        {error_comments}")
        logger.info("=============================")


if __name__ == "__main__":
    consume_messages()