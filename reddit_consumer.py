import json
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from datetime import datetime
from nltk.sentiment import SentimentIntensityAnalyzer
from transformers import pipeline
import nltk

kafka_creds = {}
with open(r'PROJECT\.venv\kafka_credentials.txt', 'r') as f:
    for line in f:
        if '=' in line:
            key, value = line.strip().split("=", 1)
            kafka_creds[key] = value

# Load MongoDB credentials
mongo_creds = {}
with open(r'PROJECT\.venv\mongo_credentials.txt', 'r') as f:
    for line in f:
        if '=' in line:
            key, value = line.strip().split("=", 1)
            mongo_creds[key] = value

print('=' *60)
print('Reddit Consumer Started')
print('=' *60)

print("\n📦 Loading sentiment analyzers...")
try:
    vader = SentimentIntensityAnalyzer()
    print("✅ VADER loaded!")
except:
    print("⚠️ Downloading VADER lexicon...")
    nltk.download('vader_lexicon')
    vader = SentimentIntensityAnalyzer()
    print("✅ VADER loaded!")

transformer = pipeline('sentiment-analysis', 
                      model="cardiffnlp/twitter-roberta-base-sentiment-latest")
print("✅ Transformer model loaded!")

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
print("✅ Kafka consumer initialized!")

print("\n🔌 Connecting to MongoDB...")
mongo_client = MongoClient(mongo_creds['connection_string'])
db = mongo_client['reddit_sentiment']
collection = db['posts']

collection.create_index('post_id', unique=True)
collection.create_index('timestamp')
collection.create_index('subreddit')
collection.create_index('transformer_label')

print(f"✅ Connected to MongoDB: {db.name}.{collection.name}")

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
        print(f"⚠️ Error analyzing sentiment: {e}")
        return None

def process_message(message):
    """Process a message from Kafka, analyze sentiment, and store in MongoDB"""
    try:
        # Parse message
        data = json.loads(message.value().decode('utf-8'))
        post_text = data.get('post_text', data.get('title', ''))
        sentiment = analyze_sentiment(post_text)
        
        if sentiment is None:
            print(f"⚠️ Skipped {data.get('post_id', 'unknown')}: No sentiment")
            return False
        
        # Add sentiment analysis results
        data['vader_score'] = sentiment['vader_score']
        data['transformer_label'] = sentiment['transformer_label']
        data['transformer_score'] = sentiment['transformer_score']
        
        # Add processing timestamp
        data['processed_at'] = datetime.utcnow().isoformat()
        
        # Insert or update in MongoDB (upsert to avoid duplicates)
        result = collection.update_one(
            {'post_id': data['post_id']},
            {'$set': data},
            upsert=True
        )
        
        # Get sentiment emoji
        sentiment_emoji = {
            'positive': '😊',
            'negative': '😞',
            'neutral': '😐'
        }.get(data['transformer_label'], '❓')
        
        if result.upserted_id:
            print(f"✓ Inserted: {data['post_id']} | r/{data['subreddit']} | {sentiment_emoji} {data['transformer_label']} ({data['transformer_score']:.2f})")
        else:
            print(f"↻ Updated: {data['post_id']} | r/{data['subreddit']} | {sentiment_emoji} {data['transformer_label']} ({data['transformer_score']:.2f})")
        
        return True
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        return False

def consume_messages():
    """Consume messages from Kafka, analyze sentiment, and store in MongoDB"""
    consumer.subscribe(['reddit-sentiment'])
    
    print("\n" + "="*60)
    print("⏳ Waiting for messages from Kafka...")
    print("Press Ctrl+C to stop")
    print("="*60 + "\n")
    
    processed_count = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'📭 Reached end of partition {msg.partition()}')
                else:
                    print(f'❌ Error: {msg.error()}')
                continue
            
            # Process message
            if process_message(msg):
                processed_count += 1
                
                # Print stats every 10 messages
                if processed_count % 10 == 0:
                    print(f"\n📊 Total processed: {processed_count} messages\n")
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Shutting down consumer...")
        print(f"📊 Total messages processed: {processed_count}")
    finally:
        consumer.close()
        mongo_client.close()
        print("✅ Consumer closed gracefully.")

if __name__ == "__main__":
    consume_messages()

