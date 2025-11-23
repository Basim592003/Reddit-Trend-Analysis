import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
import os
import logging

st.set_page_config(layout="wide", page_title="Reddit Analytics Suite")
logging.basicConfig(level=logging.INFO)
COLORS = {'positive': '#86EFAC', 'negative': '#FCA5A5', 'neutral': '#D1D5DB'}

st.markdown("""
<style>
    .stMetric {
        padding: 15px;
        border-radius: 5px;
        border: 1px solid rgba(128, 128, 128, 0.2);
    }
    div[data-testid="stExpander"] div[role="button"] p {
        font-size: 1.1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

def load_credentials():
    creds = {}
    files = ['mongo_credentials.txt']
    for filename in files:
        try:
            with open(filename, 'r') as f:
                for line in f:
                    if '=' in line:
                        key, value = line.strip().split("=", 1)
                        creds[key.strip()] = value.strip().strip('"')
        except Exception as e:
            st.error(f"Error loading {filename}: {e}")
            st.stop()
    
    uri = creds.get('connection_string', '')
    if '?' in uri:
        creds['connection_string'] = uri.split('?')[0]
    return creds

CREDS = load_credentials()
MONGO_URI = CREDS.get('connection_string')
DB_NAME = 'reddit_sentiment'

@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

@st.cache_data(ttl=10)
def load_data():
    """Fetches Posts and Comments, combines them, and handles missing values."""
    client = get_mongo_client()
    db = client[DB_NAME]
    
    posts = list(db['posts'].find().limit(3000).sort("timestamp", -1))
    df_posts = pd.DataFrame(posts)
    if not df_posts.empty:
        df_posts['type'] = 'Post'
        df_posts['link'] = df_posts.get('url', '') 
        df_posts['body_preview'] = df_posts.get('selftext', '')

    comments = list(db['comments'].find().limit(3000).sort("timestamp", -1))
    df_comments = pd.DataFrame(comments)
    if not df_comments.empty:
        df_comments['type'] = 'Comment'
        df_comments['title'] = df_comments.get('body', '').str[:100] + "..."
        df_comments['body_preview'] = df_comments.get('body', '')
        df_comments['link'] = df_comments.get('url', '')

    if df_posts.empty and df_comments.empty:
        return pd.DataFrame()
    
    df = pd.concat([df_posts, df_comments], ignore_index=True)
    
    df['vader_score'] = pd.to_numeric(df['vader_score'], errors='coerce').fillna(0.0)
    df['transformer_score'] = pd.to_numeric(df['transformer_score'], errors='coerce').fillna(0.0)
    df['transformer_label'] = df['transformer_label'].fillna('neutral').str.lower()
    df['subreddit'] = df['subreddit'].fillna('unknown')
    
    return df

df = load_data()

if df.empty:
    st.warning("No data found. Please run the Producer and Consumer scripts.")
    st.stop()

st.title("📊 Real-Time Reddit Sentiment Analytics")
st.markdown("### Executive Summary (Global Data)")

total_docs = len(df)
avg_vader = df['vader_score'].mean()
active_subs = df['subreddit'].nunique()
dominant_sent = df['transformer_label'].mode()[0].title()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Analyzed", f"{total_docs:,}")
k2.metric("Avg Sentiment (VADER)", f"{avg_vader:.2f}")
k3.metric("Dominant Sentiment", dominant_sent)
k4.metric("Active Subreddits", active_subs)

st.markdown("---")

c_left, c_right = st.columns(2)

with c_left:
    st.subheader("Global Sentiment Distribution")
    sent_counts = df['transformer_label'].value_counts().reset_index()
    sent_counts.columns = ['Sentiment', 'Count']
    
    fig_pie = px.pie(sent_counts, values='Count', names='Sentiment', 
                     color='Sentiment', color_discrete_map=COLORS, hole=0.4)
    fig_pie.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300)
    st.plotly_chart(fig_pie, use_container_width=True)

with c_right:
    st.subheader("Global Top Subreddits")
    sub_counts = df['subreddit'].value_counts().head(7).reset_index()
    sub_counts.columns = ['Subreddit', 'Volume']
    fig_bar = px.bar(sub_counts, x='Volume', y='Subreddit', orientation='h',
                     color='Volume', color_continuous_scale='Blues')
    fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(t=0, b=0, l=0, r=0), height=300)
    st.plotly_chart(fig_bar, use_container_width=True)

st.caption("Monitored Subreddits:")
subs_list = sorted(df['subreddit'].unique())
st.markdown(" ".join([f"`r/{s}`" for s in subs_list]))

st.markdown("---")

st.header("🔎 Data Explorer")

with st.container():
    f_col1, f_col2, f_col3 = st.columns([2, 1, 1])
    
    with f_col1:
        all_options = sorted(df['subreddit'].unique())
        select_all = st.checkbox("Select All Subreddits", value=True)
        
        if select_all:
            selected_subs = st.multiselect("Filter by Subreddit", all_options, default=all_options)
        else:
            selected_subs = st.multiselect("Filter by Subreddit", all_options)

    with f_col2:
        selected_types = st.multiselect("Content Type", ['Post', 'Comment'], default=['Post', 'Comment'])

    with f_col3:
        st.markdown("**Sentiment**")
        chk_pos = st.checkbox("Positive", value=True)
        chk_neu = st.checkbox("Neutral", value=True)
        chk_neg = st.checkbox("Negative", value=True)

sentiments_to_show = []
if chk_pos: sentiments_to_show.append('positive')
if chk_neu: sentiments_to_show.append('neutral')
if chk_neg: sentiments_to_show.append('negative')

filtered_df = df[
    (df['subreddit'].isin(selected_subs)) &
    (df['type'].isin(selected_types)) &
    (df['transformer_label'].isin(sentiments_to_show))
]

if not filtered_df.empty:
    st.subheader(f"Sentiment Breakdown ({len(filtered_df)} items selected)")
    
    grouped_sent = filtered_df.groupby(['subreddit', 'transformer_label']).size().reset_index(name='Count')
    
    fig_hbar = px.bar(grouped_sent, x='Count', y='subreddit', color='transformer_label',
                      orientation='h', title="Sentiment by Subreddit (Filtered View)",
                      color_discrete_map=COLORS, barmode='stack')
    fig_hbar.update_layout(yaxis={'categoryorder':'total ascending'}, height=400)
    st.plotly_chart(fig_hbar, use_container_width=True)

st.subheader("Recent Feed")

if filtered_df.empty:
    st.info("No data matches your filters.")
else:
    for i, row in filtered_df.head(10).iterrows():
        
        card_color = "gray"
        if row['transformer_label'] == 'positive': card_color = "green"
        elif row['transformer_label'] == 'negative': card_color = "red"
        
        with st.container():
            c1, c2 = st.columns([4, 1])
            with c1:
                st.markdown(f"**r/{row['subreddit']}** • u/{row['author']} • *{row['type']}*")
            with c2:
                st.markdown(f":{card_color}[**{row['transformer_label'].upper()}**]")

            if row['type'] == 'Post':
                st.markdown(f"### {row['title']}")
                if row['body_preview']:
                    st.caption(row['body_preview'][:200] + "..." if len(row['body_preview']) > 200 else row['body_preview'])
            else:
                st.markdown(f"{row['body_preview']}")

            s1, s2, s3 = st.columns([2, 2, 2])
            s1.markdown(f"**VADER:** {row['vader_score']:.3f}")
            s2.markdown(f"**AI Model:** {row['transformer_score']:.3f}")
            
            link = row['link']
            if link and not link.startswith('http'):
                link = f"https://www.reddit.com{link}"
            
            if link:
                s3.markdown(f"[🔗 Open in Reddit]({link})")
            
            st.divider()