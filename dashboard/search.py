import praw
import pandas as pd
from datetime import datetime
import pytz
from nltk.sentiment import SentimentIntensityAnalyzer
from transformers import pipeline
import streamlit as st
import os
# from dotenv import load_dotenv

# load_dotenv(dotenv_path='../.env')


def load_reddit_credentials():
    if hasattr(st, 'secrets') and 'reddit' in st.secrets:
        try:
            return {
                'client_id': st.secrets['reddit']['client_id'],
                'client_secret': st.secrets['reddit']['client_secret'],
                'user_agent': st.secrets['reddit']['user_agent']
            }
        except Exception as e:
            st.error(f"Error loading Reddit secrets: {e}")
            return None
    
    # try:
    #     client_id = os.getenv('REDDIT_CLIENT_ID')
    #     client_secret = os.getenv('REDDIT_CLIENT_SECRET')
    #     user_agent = os.getenv('REDDIT_USER_AGENT')
        
    #     if not all([client_id, client_secret, user_agent]):
    #         st.error("Reddit credentials not found in environment variables!")
    #         return None
            
    #     return {
    #         'client_id': client_id,
    #         'client_secret': client_secret,
    #         'user_agent': user_agent
    #     }
    # except Exception as e:
    #     st.error(f"Error loading reddit credentials: {e}")
    #     return None
    
@st.cache_resource
def get_reddit_client():
    creds = load_reddit_credentials()
    if not creds:
        return None
    
    try:
        reddit = praw.Reddit(
            client_id=creds.get('client_id'),
            client_secret=creds.get('client_secret'),
            user_agent=creds.get('user_agent')
        )
        return reddit
    except Exception as e:
        st.error(f"Failed to initialize Reddit client: {e}")
        return None

@st.cache_resource
def get_sentiment_analyzers():
    vader = SentimentIntensityAnalyzer()
    transformer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
    return vader, transformer

def analyze_sentiment(text, vader, transformer):
    vader_score = vader.polarity_scores(text)['compound']
    
    try:
        transformer_result = transformer(text[:512])[0]
        transformer_label = transformer_result['label'].lower()
    except:
        transformer_label = 'neutral'
    
    return vader_score, transformer_label

def fetch_and_analyze_subreddit(subreddit_name, num_posts=10, sort_by='new'):
    reddit = get_reddit_client()
    if not reddit:
        return None
    
    vader, transformer = get_sentiment_analyzers()
    posts_data = []
    
    try:
        subreddit = reddit.subreddit(subreddit_name)
        
        posts = getattr(subreddit, sort_by)(limit=num_posts)
        
        for post in posts:
            text_content = f"{post.title} {post.selftext}"
            vader_score, transformer_label = analyze_sentiment(text_content, vader, transformer)
            
            eastern = pytz.timezone('America/New_York')
            timestamp = datetime.fromtimestamp(post.created_utc, tz=pytz.UTC).astimezone(eastern)
            
            posts_data.append({
                'title': post.title,
                'author': str(post.author),
                'score': post.score,
                'num_comments': post.num_comments,
                'url': post.url,
                'permalink': f"https://www.reddit.com{post.permalink}",
                'selftext': post.selftext,
                'subreddit': subreddit_name,
                'timestamp': timestamp,
                'vader_score': vader_score,
                'transformer_label': transformer_label
            })
        
        return pd.DataFrame(posts_data)
    
    except Exception as e:
        st.error(f"Error fetching data from r/{subreddit_name}: {e}")
        return None
    