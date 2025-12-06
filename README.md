# Reddit Trend Analysis

Real-time sentiment analysis pipeline for Reddit data using Kafka streaming, MongoDB storage, and interactive dashboards.

## Project Goal

This project builds an end-to-end data pipeline that:
- Fetches Reddit posts and comments in real-time
- Streams data through Apache Kafka
- Performs sentiment analysis using NLTK VADER
- Stores processed data in MongoDB
- Visualizes trends through an interactive Streamlit dashboard

## Architecture
```mermaid
graph LR
    subgraph data_src["Data Source"]
        reddit["Reddit APISocial Media Platform"]
    end
    
    subgraph ingestion["Data Ingestion"]
        producer["Python ProducerPRAW LibraryMulti-threaded"]
    end
    
    subgraph broker["Message Broker"]
        kafka["Apache KafkaConfluent CloudSASL_SSL"]
    end
    
    subgraph processing["Stream Processing"]
        consumer["Python ConsumerNLTK VADERSentiment Analysis"]
    end
    
    subgraph storage["Data Storage"]
        mongo["MongoDB AtlasNoSQL DatabaseTTL Indexes"]
    end
    
    subgraph vis["Visualization"]
        streamlit["Streamlit DashboardReal-time AnalyticsInteractive UI"]
    end
    
    subgraph automation["Automation"]
        github["GitHub ActionsCI/CD PipelineScheduled Runs"]
    end
    
    subgraph user["End User"]
        enduser["User InterfaceWeb Browser"]
    end
    
    reddit -->|"Fetch posts& comments"| producer
    producer -->|"Streammessages"| kafka
    kafka -->|"Encryptedtransport"| consumer
    consumer -->|"Store sentimentscores"| mongo
    mongo -->|"Querydata"| streamlit
    streamlit -->|"Displayinsights"| enduser
    
    github -.->|"Every 30min"| producer
    github -.->|"Every 15min"| consumer
    
    classDef sourceStyle fill:#e8f5e9,stroke:#4caf50,stroke-width:3px,color:#1b5e20
    classDef ingestStyle fill:#e3f2fd,stroke:#2196f3,stroke-width:3px,color:#0d47a1
    classDef brokerStyle fill:#fff9c4,stroke:#fbc02d,stroke-width:3px,color:#f57f17
    classDef processStyle fill:#fce4ec,stroke:#e91e63,stroke-width:3px,color:#880e4f
    classDef storageStyle fill:#f3e5f5,stroke:#9c27b0,stroke-width:3px,color:#4a148c
    classDef visStyle fill:#ffe0b2,stroke:#ff9800,stroke-width:3px,color:#e65100
    classDef autoStyle fill:#b3e5fc,stroke:#03a9f4,stroke-width:3px,color:#01579b
    classDef userStyle fill:#eceff1,stroke:#607d8b,stroke-width:3px,color:#263238
    
    class reddit sourceStyle
    class producer ingestStyle
    class kafka brokerStyle
    class consumer processStyle
    class mongo storageStyle
    class streamlit visStyle
    class github autoStyle
    class enduser userStyle
```


## Requirements

### Software
- Python 3.10+
- MongoDB Atlas account
- Confluent Cloud Kafka account
- Reddit API credentials

### Python Dependencies
```bash
pip install -r requirements.txt
```

## Setup Instructions

### 1. Clone Repository
```bash
git clone https://github.com/Basim592003/Reddit-Trend-Analysis.git
cd Reddit-Trend-Analysis
```

### 2. Create Virtual Environment
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Download NLTK Data
```bash
python -c "import nltk; nltk.download('vader_lexicon')"
```

### 4. Configure Environment Variables

Create `.env` file in root directory:
```
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=your_user_agent

KAFKA_BOOTSTRAP_SERVERS=your_kafka_server
KAFKA_API_KEY=your_api_key
KAFKA_API_SECRET=your_api_secret

MONGO_CONNECTION_STRING=your_mongo_connection_string
```

### 5. Configure Subreddits

Edit `config.yaml` to specify subreddits to monitor:
```yaml
subreddits:
  - technology
  - news
  - worldnews
```

## Running the Pipeline

### Step 1: Start Producer
```bash
cd producer
python reddit_producer.py
```

### Step 2: Start Consumer
```bash
cd consumer
python reddit_consumer.py
```

### Step 3: Launch Dashboard
```bash
python dashboard.py
```

## Methodology

### Data Collection
- Reddit API (PRAW) fetches posts and comments from configured subreddits
- Multi-threaded producer handles rate limiting and continuous streaming
- Data serialized to JSON and published to Kafka topics

### Stream Processing
- Confluent Kafka handles message queuing with SASL_SSL security
- Consumer processes messages in real-time
- Duplicate detection via MongoDB unique indexes

### Sentiment Analysis
- NLTK VADER analyzer computes compound sentiment scores
- Range: -1 (negative) to +1 (positive)
- Scores stored with original post/comment data

### Storage
- MongoDB Atlas stores processed data
- Indexed fields: post_id, subreddit, timestamp, score
- TTL indexes for automatic data cleanup

### Visualization
- Streamlit dashboard provides real-time analytics
- Metrics: sentiment trends, top posts, subreddit comparisons
- Interactive filters and time-based analysis

## Automation

GitHub Actions workflows run automatically:
- **Producer**: Every 30 minutes
- **Consumer**: Every 15 minutes

Manual triggers available via `workflow_dispatch`

## Project Structure
```
PROJECT/
├── producer/
│   └── reddit_producer.py
├── consumer/
│   └── reddit_consumer.py
├── .github/
│   └── workflows/
│       ├── producer.yml
│       └── consumer.yml
├── reports/
├── dashboard.py
├── config.yaml
├── requirements.txt
├── .env
└── README.md
```

## Results

### Sample Output

**Producer Console:**
```
POSTED: t3_abc123 | r/technology | Score: 245
POSTED COMMENT: t1_def456 | r/technology
```

**Consumer Summary:**
```
=============================
       MONGO SUMMARY
=============================
New posts added:       127
Updated posts:         45
New comments added:    892
=============================
```

### Dashboard Features
- Real-time sentiment distribution
- Trending subreddits
- Top positive/negative posts
- Historical sentiment trends

## Troubleshooting

**Reddit API Rate Limits:**
- Producer implements exponential backoff
- Reduce subreddit count in config.yaml

**Kafka Connection Issues:**
- Verify credentials in .env
- Check Confluent Cloud cluster status

**MongoDB Storage Limits:**
- Monitor Atlas storage usage
- Implement TTL indexes for cleanup

## Future Enhancements

- Add more sentiment models (transformer-based)
- Implement real-time alerts for viral posts
- Expand to multiple social media platforms
- Deploy dashboard to cloud platform

## Contributors

Shaikh Basim - Northeastern University

## License

MIT License