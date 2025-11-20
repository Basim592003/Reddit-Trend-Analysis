import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
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
            print(f"SKIPPED (No sentiment): {identifier}")

            if msg_type == 'post':
                error_posts += 1
            else:
                error_comments += 1

            return False

        data.update({
            'vader_score': sentiment['vader_score'],
            'transformer_label': sentiment['transformer_label'],
            'transformer_score': sentiment['transformer_score'],
            'processed_at': datetime.now(timezone.utc).isoformat()
        })

        if msg_type == 'post':
            result = post.update_one(
                {'post_id': data['post_id']},
                {'$set': data},
                upsert=True
            )

            if result.upserted_id:
                print(f"NEW POST: {data['post_id']} | r/{data['subreddit']}")
                new_posts_added += 1
            else:
                print(f"UPDATED POST: {data['post_id']}")
                updated_posts += 1

        else:  
            result = comment.update_one(
                {'comment_id': data['comment_id']},
                {'$set': data},
                upsert=True
            )

            if result.upserted_id:
                print(f"NEW COMMENT: {data['comment_id']} | r/{data['subreddit']}")
                new_comments_added += 1
            else:
                print(f"UPDATED COMMENT: {data['comment_id']}")
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

        print(f"ERROR processing item {identifier}: {e}")

        if msg_type == 'post':
            error_posts += 1
        else:
            error_comments += 1

        return False


def consume_messages():
    consumer.subscribe(['reddit-sentiment'])
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

            process_message(msg)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        mongo_client.close()

        print("\n=============================")
        print("       MONGO SUMMARY")
        print("=============================")
        print(f"New posts added:       {new_posts_added}")
        print(f"Updated posts:         {updated_posts}")
        print(f"Post errors:           {error_posts}")
        print("")
        print(f"New comments added:    {new_comments_added}")
        print(f"Updated comments:      {updated_comments}")
        print(f"Comment errors:        {error_comments}")
        print("=============================")


if __name__ == "__main__":
    consume_messages()
