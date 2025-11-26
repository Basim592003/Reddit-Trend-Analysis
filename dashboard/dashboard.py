import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import os
import logging
import time
from datetime import datetime

# Install this package: pip install streamlit-autorefresh
try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st.warning("Install 'streamlit-autorefresh' for automatic dashboard updates: `pip install streamlit-autorefresh`")

# --- 1. SETUP & CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Reddit Analytics Suite")
logging.basicConfig(level=logging.INFO)

# --- AUTO-REFRESH CONFIGURATION ---
# Auto-refresh every 5 minutes (300000 milliseconds)
if HAS_AUTOREFRESH:
    count = st_autorefresh(interval=300000, limit=None, key="datarefresh")

# --- COLOR PALETTE CONFIGURATION ---
# Current Selection: Pastel / Light (Soft & Modern)
COLORS = {'positive': '#86EFAC', 'negative': '#FCA5A5', 'neutral': '#D1D5DB'}

# Custom CSS for the "Card" look in the feed
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
    """
    Load credentials from either:
    1. Streamlit secrets (when deployed on Streamlit Cloud)
    2. Local mongo_credentials.txt file (for local development)
    """
    # Try Streamlit secrets first (for cloud deployment)
    if hasattr(st, 'secrets') and 'mongo' in st.secrets:
        try:
            return {
                'connection_string': st.secrets['mongo']['connection_string']
            }
        except Exception as e:
            st.error(f"Error loading Streamlit secrets: {e}")
    
    # Fallback to local file (for development)
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
            st.error(f" Credentials not found! Please configure Streamlit secrets or create {filename}")
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
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

@st.cache_data(ttl=300)
def load_data():
    """Fetches Posts and Comments, combines them, and handles missing values."""
    client = get_mongo_client()
    db = client[DB_NAME]
    
    # Fetch Posts
    posts = list(db['posts'].find().limit(3000).sort("timestamp", -1))
    df_posts = pd.DataFrame(posts)
    if not df_posts.empty:
        df_posts['type'] = 'Post'
        df_posts['link'] = df_posts.get('permalink', df_posts.get('url', ''))  # Prefer permalink for posts
        df_posts['body_preview'] = df_posts.get('selftext', '')

    # Fetch Comments
    comments = list(db['comments'].find().limit(3000).sort("timestamp", -1))
    df_comments = pd.DataFrame(comments)
    if not df_comments.empty:
        df_comments['type'] = 'Comment'
        df_comments['title'] = df_comments.get('body', '').str[:100] + "..."
        df_comments['body_preview'] = df_comments.get('body', '')
        df_comments['link'] = df_comments.get('url', '')  # Use url for comments

    if df_posts.empty and df_comments.empty:
        return pd.DataFrame(), None
    
    df = pd.concat([df_posts, df_comments], ignore_index=True)
    
    # --- TYPE CONVERSIONS ---
    # 1. Convert 'timestamp' string to Datetime object
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    
    # 2. Ensure numerical scores are floats
    df['vader_score'] = pd.to_numeric(df['vader_score'], errors='coerce').fillna(0.0)
    
    # 3. Ensure labels are strings
    df['transformer_label'] = df['transformer_label'].fillna('neutral').str.lower()
    df['subreddit'] = df['subreddit'].fillna('unknown')
    
    # Drop rows with invalid times
    df = df.dropna(subset=['timestamp'])
    
    # Return dataframe and load timestamp
    load_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    return df, load_time

# Initialize session state for tracking
if 'refresh_count' not in st.session_state:
    st.session_state.refresh_count = 0
if 'last_manual_refresh' not in st.session_state:
    st.session_state.last_manual_refresh = None

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.markdown("### Dashboard Control")
    
    # Manual Refresh Button
    if st.button("Refresh Data Now", use_container_width=True, type="primary"):
        st.cache_data.clear()
        st.session_state.refresh_count += 1
        st.session_state.last_manual_refresh = datetime.now().strftime('%H:%M:%S')
        st.rerun()
    
    st.markdown("---")
    
    # Refresh Information
    st.markdown("### Refresh Info")
    
    if HAS_AUTOREFRESH:
        st.success("Auto-Refresh: Enabled")
        st.caption("Dashboard refreshes every 5 minutes")
    else:
        st.info(" Auto-Refresh: Disabled")
        st.caption("Install streamlit-autorefresh to enable")
    
    st.caption(f"Manual refreshes: {st.session_state.refresh_count}")
    
    if st.session_state.last_manual_refresh:
        st.caption(f"Last manual refresh: {st.session_state.last_manual_refresh}")
    
    st.markdown("---")
    
    # MongoDB Connection Status
    #st.markdown("###  Data Source")
    #try:
    #    client = get_mongo_client()
    #    client.server_info()  # Test connection
    #    st.success(" MongoDB Connected")
    #except Exception as e:
    #    st.error(" MongoDB Connection Failed")
    #   st.caption(str(e))
    
    #st.markdown("---")
    
    # Additional Info
    #st.markdown("###  Settings")
    #st.caption("**Cache TTL:** 5 minutes")
    #st.caption("**Data Limit:** 3000 items per collection")
    #st.caption("**Update Frequency:** Every 5 minutes")

# Load Global Data
df, load_time = load_data()

if df.empty:
    st.warning("No data found. Please run the Producer and Consumer scripts.")
    st.stop()

# --- DATA FRESHNESS INDICATOR ---
st.markdown(f"""
<div class="refresh-info">
     <strong>Data Last Loaded:</strong> {load_time} |  
     <strong>Next Auto-Refresh:</strong> in ~5 minutes
</div>
""", unsafe_allow_html=True)

# --- 2. EXECUTIVE SUMMARY ---
st.title("Real-Time Reddit Sentiment Analytics")
st.markdown("### Executive Summary (Global Data)")

# Calculations for KPIs
total_docs = len(df)
avg_vader = df['vader_score'].mean()
active_subs = df['subreddit'].nunique()
dominant_sent = df['transformer_label'].mode()[0].title()

# KPI Row
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Analyzed", f"{total_docs:,}")
k2.metric("Avg Sentiment (VADER)", f"{avg_vader:.2f}")
k3.metric("Dominant Sentiment", dominant_sent)
k4.metric("Active Subreddits", active_subs)

st.markdown("---")
st.subheader(f"Global Sentiment Breakdown")

def display_sentiment_bars(df):
    """Displays a custom HTML progress bar for sentiment distribution."""
    if df.empty:
        st.info("No data available for sentiment distribution.")
        return

    # 1. Calculate Percentages
    total = len(df)
    counts = df['transformer_label'].value_counts()
    
    pos_pct = (counts.get('positive', 0) / total) * 100
    neu_pct = (counts.get('neutral', 0) / total) * 100
    neg_pct = (counts.get('negative', 0) / total) * 100

    # 2. Define Colors (Matching your dashboard theme)
    pos_color = "#34D399" # Green
    neu_color = "#9CA3AF" # Gray
    neg_color = "#F87171" # Red
    bg_color = "rgba(128, 128, 128, 0.2)" # Dark/Light mode compatible track

    # 3. Custom HTML Bar Component
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

    # 4. Render the HTML
    st.subheader("Sentiment Distribution")
    st.markdown(create_bar("Positive", pos_pct, pos_color), unsafe_allow_html=True)
    st.markdown(create_bar("Neutral", neu_pct, neu_color), unsafe_allow_html=True)
    st.markdown(create_bar("Negative", neg_pct, neg_color), unsafe_allow_html=True)

display_sentiment_bars(df)  

def create_sentiment_trends(df):
    """Creates enhanced sentiment trends over time chart with smooth styling."""
    df_time = df.copy()
    df_time['time_bin'] = df_time['timestamp'].dt.floor('30min')
    
    sentiment_counts = df_time.groupby(['time_bin', 'transformer_label']).size().unstack(fill_value=0)
    sentiment_pct = sentiment_counts.div(sentiment_counts.sum(axis=1), axis=0) * 100
    
    fig = go.Figure()
    
    # Softer, more pleasant color scheme that works in both light and dark mode
    colors = {
        'positive': '#86EFAC',    # Soft pastel green
        'neutral': '#A5B4FC',     # Soft pastel blue
        'negative': '#FCA5A5'     # Soft pastel red/coral
    }
    
    # Darker line colors for highlighting
    line_colors = {
        'positive': '#22C55E',    # Darker green
        'neutral': '#6366F1',     # Darker blue
        'negative': '#EF4444'     # Darker red
    }
    
    # Calculate cumulative values for proper stacking with extra space
    cumulative = sentiment_pct[['positive']].copy()
    cumulative['neutral'] = sentiment_pct['positive'] + sentiment_pct['neutral']
    # Add 10% buffer to show negative wave
    cumulative['negative'] = sentiment_pct['positive'] + sentiment_pct['neutral'] + (sentiment_pct['negative'] * 1.1)
    
    # Add positive area
    fig.add_trace(go.Scatter(
        x=sentiment_pct.index,
        y=sentiment_pct['positive'],
        name='Positive',
        fill='tozeroy',
        mode='lines',
        line=dict(
            color=line_colors['positive'],
            width=3,
            shape='spline',
            smoothing=1.3
        ),
        fillcolor=colors['positive'],
        opacity=0.65,
        showlegend=True,
        hovertemplate='<b>Positive</b><br>%{x|%b %d, %H:%M} - %{y:.1f}%<br><extra></extra>'
    ))
    
    # Add neutral area
    fig.add_trace(go.Scatter(
        x=sentiment_pct.index,
        y=cumulative['neutral'],
        name='Neutral',
        fill='tonexty',
        mode='lines',
        line=dict(
            color=line_colors['neutral'],
            width=3,
            shape='spline',
            smoothing=1.3
        ),
        fillcolor=colors['neutral'],
        opacity=0.65,
        showlegend=True,
        hovertemplate='<b>Neutral</b><br>%{x|%b %d, %H:%M} - %{customdata:.1f}%<br><extra></extra>',
        customdata=sentiment_pct['neutral']
    ))
    
    # Add negative area with extra height for wave visibility
    fig.add_trace(go.Scatter(
        x=sentiment_pct.index,
        y=cumulative['negative'],
        name='Negative',
        fill='tonexty',
        mode='lines',
        line=dict(
            color=line_colors['negative'],
            width=3,
            shape='spline',
            smoothing=1.3
        ),
        fillcolor=colors['negative'],
        opacity=0.65,
        showlegend=True,
        hovertemplate='<b>Negative</b><br>%{x|%b %d, %H:%M} - %{customdata:.1f}%<br><extra></extra>',
        customdata=sentiment_pct['negative']
    ))
    
    fig.update_layout(
        title={
            'text': 'Sentiment Trends Over Time',
            'font': {'size': 18, 'family': 'Inter, Arial, sans-serif'},
            'y': 0.98,
            'x': 0.02,
            'xanchor': 'left',
            'yanchor': 'top'
        },
        xaxis_title='Time',
        yaxis_title='Sentiment Distribution (%)',
        hovermode='x unified',
        height=480,
        margin=dict(t=50, b=120, l=70, r=30),  # Increased bottom margin for more gap
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, Arial, sans-serif', size=12),
        xaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128, 128, 128, 0.2)',
            showline=True,
            linewidth=2,
            linecolor='rgba(128, 128, 128, 0.3)',
            tickformat='%b %d\n%H:%M',
            tickangle=0,
            type='date',
            rangeslider=dict(visible=False),
            tickmode='auto',
            nticks=12,
            ticklabelmode='period'
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128, 128, 128, 0.2)',
            showline=True,
            linewidth=2,
            linecolor='rgba(128, 128, 128, 0.3)',
            range=[0, 120],
            dtick=20,
            tickmode='linear'
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=-0.35,  # Moved further down for more gap
            xanchor='center',
            x=0.5,
            font=dict(size=13),
            bgcolor='rgba(0,0,0,0)',
            bordercolor='rgba(128, 128, 128, 0.3)',
            borderwidth=1,
            itemclick=False,  # Disable click to hide/show traces
            itemdoubleclick=False  # Disable double-click to isolate trace
        ),
        hoverlabel=dict(
            bgcolor='rgba(50, 50, 50, 0.95)',
            font_size=12,
            font_family='Inter, Arial, sans-serif',
            font_color='white',
            bordercolor='rgba(128, 128, 128, 0.5)'
        )
    )
    
    return fig

st.subheader("Sentiment Trends (24h)")
fig_trends = create_sentiment_trends(df)
st.plotly_chart(fig_trends, use_container_width=True)
st.markdown("---")

# Fixed Visualizations Row
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

# Monitored Subreddits
st.caption("Monitored Subreddits:")
subs_list = sorted(df['subreddit'].unique())
st.markdown(" ".join([f"`r/{s}`" for s in subs_list]))

st.markdown("---")

# --- 3. INTERACTIVE DATA FEED (FILTERABLE) ---

st.header("Data Explorer")

# Filters Section - Vertical Layout
st.markdown("### Filters")

# Subreddit Filter
all_options = sorted(df['subreddit'].unique())
select_all = st.checkbox("Select All Subreddits", value=True)

if select_all:
    selected_subs = st.multiselect("Filter by Subreddit", all_options, default=all_options)
else:
    selected_subs = st.multiselect("Filter by Subreddit", all_options)

# Content Type Filter
selected_types = st.multiselect("Content Type", ['Post', 'Comment'], default=['Post', 'Comment'])

# Sentiment Filter
st.markdown("**Sentiment**")
chk_pos = st.checkbox("Positive", value=True)
chk_neu = st.checkbox("Neutral", value=True)
chk_neg = st.checkbox("Negative", value=True)

st.markdown("---")
    
    
# Build Filter Lists
sentiments_to_show = []
if chk_pos: sentiments_to_show.append('positive')
if chk_neu: sentiments_to_show.append('neutral')
if chk_neg: sentiments_to_show.append('negative')

# Apply Filters
filtered_df = df[
    (df['subreddit'].isin(selected_subs)) &
    (df['type'].isin(selected_types)) &
    (df['transformer_label'].isin(sentiments_to_show))
]

# --- 4. FILTERED ANALYSIS VISUAL ---
if not filtered_df.empty:
    st.subheader(f"Sentiment Breakdown")
    
    # Horizontal Bar Chart for Filtered Selection
    grouped_sent = filtered_df.groupby(['subreddit', 'transformer_label']).size().reset_index(name='Count')
    
    fig_hbar = px.bar(grouped_sent, x='Count', y='subreddit', color='transformer_label',
                      orientation='h', title="Sentiment by Subreddit (Filtered View)",
                      color_discrete_map=COLORS, barmode='stack')
    fig_hbar.update_layout(
        yaxis={'categoryorder':'total ascending'},
        height=max(100, len(grouped_sent['subreddit'].unique()) * 20),  
        margin=dict(l=50, r=50, t=50, b=50)  
    )
    st.plotly_chart(fig_hbar, use_container_width=True)

# --- 5. RECENT DATA FEED CARDS ---
st.subheader("Recent Live Feed")
st.caption("Showing the most recent posts from all subreddits, regardless of filters")

# Get top 10 most recent posts only (not comments) sorted by timestamp
recent_posts = df[df['type'] == 'Post'].sort_values('timestamp', ascending=False).head(10)

if recent_posts.empty:
    st.info("No recent posts available.")
else:
    # Display recent 10 posts
    for i, row in recent_posts.iterrows():
        
        # Determine Card Color based on Sentiment for visual badge
        card_color = "gray"
        if row['transformer_label'] == 'positive': card_color = "green"
        elif row['transformer_label'] == 'negative': card_color = "red"
        
        # Use st.container for the card layout
        with st.container():
            # Header Line
            c1, c2 = st.columns([4, 1])
            with c1:
                st.markdown(f"**r/{row['subreddit']}** • u/{row['author']} • *{row['type']}*")
            with c2:
                # Badge style sentiment
                st.markdown(f":{card_color}[**{row['transformer_label'].upper()}**]")

            # Title / Content
            st.markdown(f"### {row['title']}")
            if row['body_preview']:
                st.caption(row['body_preview'][:200] + "..." if len(row['body_preview']) > 200 else row['body_preview'])

            # Scores and Link
            s1, s2, s3 = st.columns([2, 2, 2])
            s1.markdown(f"**VADER:** {row['vader_score']:.3f}")
            s2.markdown(f"**Posted:** {row['timestamp'].strftime('%Y-%m-%d %H:%M')}")
            
            # Handle Link - use 'permalink' for posts, 'url' for comments
            if row['type'] == 'Post':
                link = row.get('permalink', row.get('url', ''))
            else:
                link = row.get('url', '')
            
            # Add reddit domain if needed
            if link and not link.startswith('http'):
                link = f"https://www.reddit.com{link}"
            
            if link:
                s3.markdown(f"[View on Reddit]({link})")
            
            st.divider()

# --- FOOTER ---
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: gray; padding: 20px;">
    <p>Real-Time Reddit Sentiment Analytics | Data refreshes automatically every 5 minutes</p>
    <p>Use the refresh button in the sidebar to manually update data</p>
</div>
""", unsafe_allow_html=True)
