import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import os
import logging
import time
from datetime import datetime
import pytz
from search import fetch_and_analyze_subreddit

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st.warning("Install 'streamlit-autorefresh' for automatic dashboard updates: `pip install streamlit-autorefresh`")

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
    client = get_mongo_client()
    db = client[DB_NAME]
    
    posts = list(db['posts'].find().limit(500).sort("timestamp", -1))
    df_posts = pd.DataFrame(posts)
    if not df_posts.empty:
        df_posts['type'] = 'Post'
        df_posts['link'] = df_posts.get('permalink', df_posts.get('url', ''))
        df_posts['body_preview'] = df_posts.get('selftext', '')

    comments = list(db['comments'].find().limit(500).sort("timestamp", -1))
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
        eastern = pytz.timezone('America/New_York')
        df['timestamp'] = df['timestamp'].dt.tz_convert(eastern)
    
    df['vader_score'] = pd.to_numeric(df['vader_score'], errors='coerce').fillna(0.0)
    df['transformer_label'] = df['transformer_label'].fillna('neutral').str.lower()
    df['subreddit'] = df['subreddit'].fillna('unknown')
    df = df.dropna(subset=['timestamp'])
    
    eastern = pytz.timezone('America/New_York')
    load_time = datetime.now(eastern).strftime('%Y-%m-%d %H:%M:%S')
        
    return df, load_time

if 'refresh_count' not in st.session_state:
    st.session_state.refresh_count = 0
if 'last_manual_refresh' not in st.session_state:
    st.session_state.last_manual_refresh = None

with st.sidebar:
    st.markdown("### Dashboard Control")
    
    if st.button("Refresh Data Now", use_container_width=True, type="primary"):
        st.cache_data.clear()
        st.session_state.refresh_count += 1
        eastern = pytz.timezone('America/New_York')
        st.session_state.last_manual_refresh = datetime.now(eastern).strftime('%H:%M:%S')
        st.rerun()
    
    st.markdown("---")
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

df, load_time = load_data()

if df.empty:
    st.warning("No data found. Please run the Producer and Consumer scripts.")
    st.stop()

st.markdown(f"""
<div class="refresh-info">
     <strong>Data Last Loaded:</strong> {load_time} |  
     <strong>Next Auto-Refresh:</strong> in ~5 minutes
</div>
""", unsafe_allow_html=True)

st.title("Real-Time Reddit Sentiment Analytics")

tab1, tab2 = st.tabs(["Dashboard", "Search Reddit"])

with tab1:
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
    st.subheader(f"Global Sentiment Breakdown")

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
        
        colors = {
            'positive': '#86EFAC',
            'neutral': '#A5B4FC',
            'negative': '#FCA5A5'
        }
        
        line_colors = {
            'positive': '#22C55E',
            'neutral': '#6366F1',
            'negative': '#EF4444'
        }
        
        cumulative = sentiment_pct[['positive']].copy()
        cumulative['neutral'] = sentiment_pct['positive'] + sentiment_pct['neutral']
        cumulative['negative'] = sentiment_pct['positive'] + sentiment_pct['neutral'] + (sentiment_pct['negative'] * 1.1)
        
        fig.add_trace(go.Scatter(
            x=sentiment_pct.index,
            y=sentiment_pct['positive'],
            name='Positive',
            fill='tozeroy',
            mode='lines',
            line=dict(color=line_colors['positive'], width=3, shape='spline', smoothing=1.3),
            fillcolor=colors['positive'],
            opacity=0.65,
            showlegend=True,
            hovertemplate='<b>Positive</b><br>%{x|%b %d, %H:%M} - %{y:.1f}%<br><extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=sentiment_pct.index,
            y=cumulative['neutral'],
            name='Neutral',
            fill='tonexty',
            mode='lines',
            line=dict(color=line_colors['neutral'], width=3, shape='spline', smoothing=1.3),
            fillcolor=colors['neutral'],
            opacity=0.65,
            showlegend=True,
            hovertemplate='<b>Neutral</b><br>%{x|%b %d, %H:%M} - %{customdata:.1f}%<br><extra></extra>',
            customdata=sentiment_pct['neutral']
        ))
        
        fig.add_trace(go.Scatter(
            x=sentiment_pct.index,
            y=cumulative['negative'],
            name='Negative',
            fill='tonexty',
            mode='lines',
            line=dict(color=line_colors['negative'], width=3, shape='spline', smoothing=1.3),
            fillcolor=colors['negative'],
            opacity=0.65,
            showlegend=True,
            hovertemplate='<b>Negative</b><br>%{x|%b %d, %H:%M} - %{customdata:.1f}%<br><extra></extra>',
            customdata=sentiment_pct['negative']
        ))
        
        fig.update_layout(
            title={'text': 'Sentiment Trends Over Time', 'font': {'size': 18, 'family': 'Inter, Arial, sans-serif'}, 'y': 0.98, 'x': 0.02, 'xanchor': 'left', 'yanchor': 'top'},
            xaxis_title='Time',
            yaxis_title='Sentiment Distribution (%)',
            hovermode='x unified',
            height=480,
            margin=dict(t=50, b=120, l=70, r=30),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Inter, Arial, sans-serif', size=12),
            xaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)', showline=True, linewidth=2, linecolor='rgba(128, 128, 128, 0.3)', tickformat='%b %d\n%H:%M', tickangle=0, type='date', rangeslider=dict(visible=False), tickmode='auto', nticks=12, ticklabelmode='period'),
            yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)', showline=True, linewidth=2, linecolor='rgba(128, 128, 128, 0.3)', range=[0, 120], dtick=20, tickmode='linear'),
            legend=dict(orientation='h', yanchor='bottom', y=-0.35, xanchor='center', x=0.5, font=dict(size=13), bgcolor='rgba(0,0,0,0)', bordercolor='rgba(128, 128, 128, 0.3)', borderwidth=1, itemclick=False, itemdoubleclick=False),
            hoverlabel=dict(bgcolor='rgba(50, 50, 50, 0.95)', font_size=12, font_family='Inter, Arial, sans-serif', font_color='white', bordercolor='rgba(128, 128, 128, 0.5)')
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
        
        fig_pie = px.pie(sent_counts, values='Count', names='Sentiment', color='Sentiment', color_discrete_map=COLORS, hole=0.4)
        fig_pie.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300)
        st.plotly_chart(fig_pie, use_container_width=True)

    with c_right:
        st.subheader("Global Top Subreddits")
        sub_counts = df['subreddit'].value_counts().head(7).reset_index()
        sub_counts.columns = ['Subreddit', 'Volume']
        fig_bar = px.bar(sub_counts, x='Volume', y='Subreddit', orientation='h', color='Volume', color_continuous_scale='Blues')
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(t=0, b=0, l=0, r=0), height=300)
        st.plotly_chart(fig_bar, use_container_width=True)

    st.caption("Monitored Subreddits:")
    subs_list = sorted(df['subreddit'].unique())
    st.markdown(" ".join([f"`r/{s}`" for s in subs_list]))

    st.markdown("---")

    st.header("Data Explorer")
    st.markdown("### Filters")

    all_options = sorted(df['subreddit'].unique())
    select_all = st.checkbox("Select All Subreddits", value=True)

    if select_all:
        selected_subs = st.multiselect("Filter by Subreddit", all_options, default=all_options)
    else:
        selected_subs = st.multiselect("Filter by Subreddit", all_options)

    selected_types = st.multiselect("Content Type", ['Post', 'Comment'], default=['Post', 'Comment'])

    st.markdown("**Sentiment**")
    chk_pos = st.checkbox("Positive", value=True)
    chk_neu = st.checkbox("Neutral", value=True)
    chk_neg = st.checkbox("Negative", value=True)

    st.markdown("---")
        
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
        st.subheader(f"Sentiment Breakdown")
        
        grouped_sent = filtered_df.groupby(['subreddit', 'transformer_label']).size().reset_index(name='Count')
        
        fig_hbar = px.bar(grouped_sent, x='Count', y='subreddit', color='transformer_label', orientation='h', title="Sentiment by Subreddit (Filtered View)", color_discrete_map=COLORS, barmode='stack')
        fig_hbar.update_layout(yaxis={'categoryorder':'total ascending'}, height=max(100, len(grouped_sent['subreddit'].unique()) * 20), margin=dict(l=50, r=50, t=50, b=50))
        st.plotly_chart(fig_hbar, use_container_width=True)

    st.subheader("Recent Live Feed")
    st.caption("Showing the most recent posts from all subreddits, regardless of filters")

    recent_posts = df[df['type'] == 'Post'].sort_values('timestamp', ascending=False).head(10)

    if recent_posts.empty:
        st.info("No recent posts available.")
    else:
        for i, row in recent_posts.iterrows():
            card_color = "gray"
            if row['transformer_label'] == 'positive': card_color = "green"
            elif row['transformer_label'] == 'negative': card_color = "red"
            
            with st.container():
                c1, c2 = st.columns([4, 1])
                with c1:
                    st.markdown(f"**r/{row['subreddit']}** • u/{row['author']} • *{row['type']}*")
                with c2:
                    st.markdown(f":{card_color}[**{row['transformer_label'].upper()}**]")

                st.markdown(f"### {row['title']}")
                if row['body_preview']:
                    st.caption(row['body_preview'][:200] + "..." if len(row['body_preview']) > 200 else row['body_preview'])

                s1, s2, s3 = st.columns([2, 2, 2])
                s1.markdown(f"**VADER:** {row['vader_score']:.3f}")
                s2.markdown(f"**Posted:** {row['timestamp'].strftime('%Y-%m-%d %H:%M')}")
                
                if row['type'] == 'Post':
                    link = row.get('permalink', row.get('url', ''))
                else:
                    link = row.get('url', '')
                
                if link and not link.startswith('http'):
                    link = f"https://www.reddit.com{link}"
                
                if link:
                    s3.markdown(f"[View on Reddit]({link})")
                
                st.divider()

with tab2:
    st.markdown("### Search Reddit")
    st.markdown("Search any subreddit and analyze sentiment of recent posts")
    
    if 'search_results' not in st.session_state:
        st.session_state.search_results = None
    
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        subreddit_input = st.text_input("Enter Subreddit Name", placeholder="e.g., python, machinelearning, news")
    
    with col2:
        num_posts = st.slider("Number of Posts", min_value=1, max_value=25, value=10)
    
    with col3:
        sort_by = st.selectbox("Sort by", ["Hot", "New", "Top", "Rising"])
    
    if st.button("Search and Analyze", type="primary"):
        if subreddit_input:
            with st.spinner(f"Fetching and analyzing posts from r/{subreddit_input}..."):
                st.session_state.search_results = fetch_and_analyze_subreddit(subreddit_input, num_posts, sort_by.lower())
        else:
            st.warning("Please enter a subreddit name")
    
    if st.session_state.search_results is not None and not st.session_state.search_results.empty:
        search_results = st.session_state.search_results
        
        st.success(f"Successfully fetched {len(search_results)} posts")
        
        st.markdown("---")
        st.subheader("Search Results Summary")
        
        k1, k2, k3 = st.columns(3)
        k1.metric("Total Posts", len(search_results))
        k2.metric("Avg VADER Score", f"{search_results['vader_score'].mean():.2f}")
        k3.metric("Dominant Sentiment", search_results['transformer_label'].mode()[0].title())
        
        st.markdown("---")
        
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("Sentiment Distribution")
            sent_counts = search_results['transformer_label'].value_counts().reset_index()
            sent_counts.columns = ['Sentiment', 'Count']
            
            fig_pie = px.pie(sent_counts, values='Count', names='Sentiment', 
                           color='Sentiment', color_discrete_map=COLORS, hole=0.4)
            fig_pie.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300)
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col_right:
            st.subheader("VADER Score Distribution")
            fig_hist = px.histogram(search_results, x='vader_score', nbins=20, 
                                  color_discrete_sequence=['#6366F1'])
            fig_hist.update_layout(
                xaxis_title="VADER Score",
                yaxis_title="Count",
                margin=dict(t=0, b=0, l=0, r=0),
                height=300
            )
            st.plotly_chart(fig_hist, use_container_width=True)
        
        st.markdown("---")
        st.subheader("Posts")
        
        for idx, row in search_results.iterrows():
            card_color = "gray"
            if row['transformer_label'] == 'positive': card_color = "green"
            elif row['transformer_label'] == 'negative': card_color = "red"
            
            with st.container():
                c1, c2 = st.columns([4, 1])
                with c1:
                    st.markdown(f"**r/{row['subreddit']}** • u/{row['author']}")
                with c2:
                    st.markdown(f":{card_color}[**{row['transformer_label'].upper()}**]")
                
                st.markdown(f"### {row['title']}")
                
                if row['selftext']:
                    st.caption(row['selftext'][:200] + "..." if len(row['selftext']) > 200 else row['selftext'])
                
                s1, s2, s3 = st.columns([2, 2, 2])
                s1.markdown(f"**VADER:** {row['vader_score']:.3f}")
                #s2.markdown(f"**Score:** {row['score']}")
                s2.markdown(f"**Comments:** {row['num_comments']}")
                s3.markdown(f"[View on Reddit]({row['permalink']})")
                
                st.divider()
    elif st.session_state.search_results is not None:
        st.error("Could not fetch data. Please check the subreddit name and try again.")

st.markdown("---")
st.markdown("""
<div style="text-align: center; color: gray; padding: 20px;">
    <p>Real-Time Reddit Sentiment Analytics | Data refreshes automatically every 5 minutes</p>
    <p>Use the refresh button in the sidebar to manually update data</p>
</div>
""", unsafe_allow_html=True)
