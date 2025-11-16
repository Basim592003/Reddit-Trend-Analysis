import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from datetime import datetime
from nltk.sentiment import SentimentIntensityAnalyzer
from transformers import pipeline
import nltk

kafka_creds = {}
with open(r'kafka_credentials.txt', 'r') as f:
    for line in f:
        if '=' in line:
            key, value = line.strip().split("=", 1)
            kafka_creds[key] = value

mongo_creds = {}
with open(r'mongo_credentials.txt', 'r') as f:
    for line in f:
        if '=' in line:
            key, value = line.strip().split("=", 1)
            mongo_creds[key] = value

try:
    vader = SentimentIntensityAnalyzer()
except:
    nltk.download('vader_lexicon')
    vader = SentimentIntensityAnalyzer()

transformer = pipeline('sentiment-analysis', 
                      model="cardiffnlp/twitter-roberta-base-sentiment-latest")

kafka_config = {
    'bootstrap.servers': kafka_creds['bootstrap_servers'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': kafka_creds['api_key'],
    'sasl.password': kafka_creds['api_secret'],
    'group.id': 'reddit-sentiment-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
}
consumer = Consumer(kafka_config)

try:
    mongo_client = MongoClient(mongo_creds['connection_string'])
    db = mongo_client['reddit_sentiment']
    post = db['posts']
    comment = db['comments']
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")

post.create_index('post_id', unique=True)
post.create_index('timestamp')
post.create_index('subreddit')
post.create_index('score')
post.create_index('transformer_label')

comment.create_index('comment_id', unique=True)
comment.create_index('post_id')
comment.create_index('timestamp')
comment.create_index('subreddit')
comment.create_index('score')
comment.create_index('transformer_label')

print(f"Connected to MongoDB: {db.name}.{post.name}")
print(f"Connected to MongoDB: {db.name}.{comment.name}")

def analyze_sentiment(text):
    """Analyze sentiment using both VADER and transformer"""
    if not text or not text.strip():
        return None
    
    try:
        vader_score = vader.polarity_scores(text)['compound']
        trans_result = transformer(text[:512])[0]
        trans_label = trans_result['label']
        trans_score = trans_result['score']
        return {
            'vader_score': vader_score,
            'transformer_label': trans_label,
            'transformer_score': trans_score
        }
    except Exception as e:
        print(f"Error analyzing sentiment: {e}")
        return None

def process_message(message):
    """Process a message from Kafka, analyze sentiment, and store in MongoDB"""
    try:
        data = json.loads(message.value().decode('utf-8'))
        if data.get('type') == 'post':
            post_text = data.get('post_text', data.get('title', ''))
        elif data.get('type') == 'comment':
            post_text = data.get('body', '')
        
        sentiment = analyze_sentiment(post_text)
        
        if sentiment is None:
            item_id = data.get('post_id') or data.get('comment_id')
            print(f" Skipped {item_id}: No sentiment")
            return False

        
        data['vader_score'] = sentiment['vader_score']
        data['transformer_label'] = sentiment['transformer_label']
        data['transformer_score'] = sentiment['transformer_score']
        data['processed_at'] = datetime.now(timezone.utc).isoformat()
    
        
        sentiment_emoji = {
            'positive': '😊',
            'negative': '😞',
            'neutral': '😐'
        }.get(data['transformer_label'], '❓')
        
        if data.get('type') == 'post':
            result = post.update_one(
            {'post_id': data['post_id']},
            {'$set': data},
            upsert=True
        )
        
            if result.upserted_id:
                print(f" Post: {data['post_id']} | r/{data['subreddit']} | {sentiment_emoji} {data['transformer_label']}")
            else:
                print(f" Updated: {data['post_id']}")

        elif data.get('type') == 'comment':
        
            result = comment.update_one(
                {'comment_id': data['comment_id']},
                {'$set': data},
                upsert=True
            )
        if result.upserted_id:
            print(f" Comment: {data['comment_id']} | r/{data['subreddit']} | {sentiment_emoji} {data['transformer_label']}")
        else:
            print(f" Updated: {data['comment_id']}")
        
        return True
    except Exception as e:
        print(f" Error processing message: {e}")
        return False


def consume_messages():
    consumer.subscribe(['reddit-sentiment'])
    
    posts_count = 0
    comments_count = 0
    empty_polls = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                empty_polls += 1
                if empty_polls > 10:
                    break
                continue
            
            empty_polls = 0
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                else:
                    continue
            
            if process_message(msg):
                data = json.loads(msg.value().decode('utf-8'))
                if data.get('type') == 'post':
                    posts_count += 1
                elif data.get('type') == 'comment':
                    comments_count += 1
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        mongo_client.close()


if __name__ == "__main__":
    consume_messages()

