import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import os
import logging
import time
from datetime import datetime

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st.warning("Install 'streamlit-autorefresh' for automatic dashboard updates: pip install streamlit-autorefresh")

st.set_page_config(layout="wide", page_title="Reddit Analytics Suite")
logging.basicConfig(level=logging.INFO)

if HAS_AUTOREFRESH:
    count = st_autorefresh(interval=300000, limit=None, key="datarefresh")

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
    .refresh-info {
        background-color: rgba(128, 128, 128, 0.1);
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 10px;
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

def load_credentials():
    if hasattr(st, 'secrets') and 'mongo' in st.secrets:
        try:
            return {
                'connection_string': st.secrets['mongo']['connection_string']
            }
        except Exception as e:
            st.error(f"Error loading Streamlit secrets: {e}")
    
    creds = {}
    files = ['mongo_credentials.txt']
    for filename in files:
        try:
            with open(filename, 'r') as f:
                for line in f:
                    if '=' in line:
                        key, value = line.strip().split("=", 1)
                        creds[key.strip()] = value.strip().strip('"')
        except FileNotFoundError:
            st.error(f"❌ Credentials not found! Please configure Streamlit secrets or create {filename}")
            st.stop()
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
    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=30000,
            socketTimeoutMS=30000,
            tls=True,
            tlsAllowInvalidCertificates=False,
            retryWrites=True,
            w='majority'
        )
        client.admin.command('ping')
        return client
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {e}")
        st.info("Please check your connection string and MongoDB Atlas network settings.")
        raise

@st.cache_data(ttl=300)
def load_data():
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
        return pd.DataFrame(), None
    
    df = pd.concat([df_posts, df_comments], ignore_index=True)
    
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    
    df['vader_score'] = pd.to_numeric(df['vader_score'], errors='coerce').fillna(0.0)
    df['transformer_label'] = df['transformer_label'].fillna('neutral').str.lower()
    df['subreddit'] = df['subreddit'].fillna('unknown')
    df = df.dropna(subset=['timestamp'])
    
    load_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    return df, load_time

if 'refresh_count' not in st.session_state:
    st.session_state.refresh_count = 0
if 'last_manual_refresh' not in st.session_state:
    st.session_state.last_manual_refresh = None

with st.sidebar:
    st.markdown("### 🎛 Dashboard Controls")
    
    if st.button("🔄 Refresh Data Now", use_container_width=True, type="primary"):
        st.cache_data.clear()
        st.session_state.refresh_count += 1
        st.session_state.last_manual_refresh = datetime.now().strftime('%H:%M:%S')
        st.rerun()
    
    st.markdown("---")
    st.markdown("### ℹ Refresh Info")
    
    if HAS_AUTOREFRESH:
        st.success("✅ Auto-refresh: Enabled")
        st.caption("Dashboard refreshes every 5 minutes")
    else:
        st.info("ℹ Auto-refresh: Disabled")
        st.caption("Install streamlit-autorefresh to enable")
    
    st.caption(f"Manual refreshes: {st.session_state.refresh_count}")
    
    if st.session_state.last_manual_refresh:
        st.caption(f"Last manual refresh: {st.session_state.last_manual_refresh}")
    
    st.markdown("---")
    st.markdown("### 📊 Data Source")
    try:
        client = get_mongo_client()
        client.server_info()
        st.success("✅ MongoDB Connected")
    except Exception as e:
        st.error("❌ MongoDB Connection Failed")
        st.caption(str(e))
    
    st.markdown("---")
    st.markdown("### ⚙ Settings")
    st.caption("*Cache TTL:* 5 minutes")
    st.caption("*Data Limit:* 3000 items per collection")
    st.caption("*Update Frequency:* Every 5 minutes")

df, load_time = load_data()

if df.empty:
    st.warning("No data found. Please run the Producer and Consumer scripts.")
    st.stop()

st.markdown(f"""
<div class="refresh-info">
    📅 <strong>Data Last Loaded:</strong> {load_time} | 
    📈 <strong>Total Records:</strong> {len(df):,} | 
    🔄 <strong>Next Auto-Refresh:</strong> in ~5 minutes
</div>
""", unsafe_allow_html=True)

st.title("Real-Time Reddit Sentiment Analytics")
st.markdown("### Executive Summary (ddddddddddddddd)")

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
st.subheader(f"Global Sentiment Breakdown ({len(df)} items selected)")

def display_sentiment_bars(df):
    if df.empty:
        st.info("No data available for sentiment distribution.")
        return

    total = len(df)
    counts = df['transformer_label'].value_counts()
    
    pos_pct = (counts.get('positive', 0) / total) * 100
    neu_pct = (counts.get('neutral', 0) / total) * 100
    neg_pct = (counts.get('negative', 0) / total) * 100

    pos_color = "#34D399"
    neu_color = "#9CA3AF"
    neg_color = "#F87171"
    bg_color = "rgba(128, 128, 128, 0.2)"

    def create_bar(label, pct, color):
        return f"""
        <div style="margin-bottom: 15px;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 5px; font-weight: 600;">
                <span>{label}</span>
                <span>{pct:.0f}%</span>
            </div>
            <div style="width: 100%; background-color: {bg_color}; border-radius: 10px; height: 10px;">
                <div style="width: {pct}%; background-color: {color}; height: 10px; border-radius: 10px;"></div>
            </div>
        </div>
        """

    st.subheader("Sentiment Distribution")
    st.markdown(create_bar("Positive", pos_pct, pos_color), unsafe_allow_html=True)
    st.markdown(create_bar("Neutral", neu_pct, neu_color), unsafe_allow_html=True)
    st.markdown(create_bar("Negative", neg_pct, neg_color), unsafe_allow_html=True)

display_sentiment_bars(df)

def create_sentiment_trends(df):
    df_time = df.copy()
    df_time['time_bin'] = df_time['timestamp'].dt.floor('30min')
    
    sentiment_counts = df_time.groupby(['time_bin', 'transformer_label']).size().unstack(fill_value=0)
    sentiment_pct = sentiment_counts.div(sentiment_counts.sum(axis=1), axis=0) * 100
    
    fig = go.Figure()
    
    colors = {'positive': '#86EFAC', 'neutral': '#D1D5DB', 'negative': '#FCA5A5'}
    
    for sentiment in ['positive', 'neutral', 'negative']:
        if sentiment in sentiment_pct.columns:
            fig.add_trace(go.Scatter(
                x=sentiment_pct.index,
                y=sentiment_pct[sentiment],
                name=sentiment,
                fill='tonexty' if sentiment != 'positive' else 'tozeroy',
                line=dict(color=colors[sentiment], width=2),
                fillcolor=colors[sentiment],
                opacity=0.7,
                stackgroup='one'
            ))
    
    fig.update_layout(
        title='Sentiment Trends Over Time',
        xaxis_title='Time',
        yaxis_title='Posts (%)',
        hovermode='x unified',
        height=400,
        margin=dict(t=40, b=40, l=40, r=40)
    )
    
    return fig

st.subheader("Sentiment Trends (24h)")
fig_trends = create_sentiment_trends(df)
st.plotly_chart(fig_trends, use_container_width=True)

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

subs_list = sorted(df['subreddit'].unique())
st.caption("Monitored Subreddits:")
st.markdown(" ".join([f"r/{s}" for s in subs_list]))

st.markdown("---")

st.header("Data Explorer")

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
        st.markdown("*Sentiment*")
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

st.subheader("Recent Live Feed")

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
                st.markdown(f"*r/{row['subreddit']}* • u/{row['author']} • {row['type']}")
            with c2:
                st.markdown(f":{card_color}[{row['transformer_label'].upper()}]")

            if row['type'] == 'Post':
                st.markdown(f"### {row['title']}")
                if row['body_preview']:
                    st.caption(row['body_preview'][:200] + "..." if len(row['body_preview']) > 200 else row['body_preview'])
            else:
                st.markdown(f"{row['body_preview']}")

            s1, s2, s3 = st.columns([2, 2, 2])
            s1.markdown(f"*VADER:* {row['vader_score']:.3f}")
            
            link = row['link']
            if link and not link.startswith('http'):
                link = f"https://www.reddit.com{link}"
            
            if link:
                s3.markdown(f"[View on Reddit]({link})")
            
            st.divider()

st.markdown("---")
st.markdown("""
<div style="text-align: center; color: gray; padding: 20px;">
    <p>Reddit Analytics Suite | Data refreshes automatically every 5 minutes</p>
    <p>Use the refresh button in the sidebar to manually update data</p>
</div>
""", unsafe_allow_html=True)