===========================================================
BMDS2013 Data Engineering Assignment – readme.txt
===========================================================

Team Reference: S3G4-2
Submission Date: 29th August 2025

-----------------------------------------------------------
1. Project Title:
-----------------------------------------------------------
Integrating Financial Data and Social Sentiment for Real-Time Insights into Malaysia’s Stock Market (SDG 8: Decent Work and Economic Growth)

-----------------------------------------------------------
2. Project Folder Structure:
-----------------------------------------------------------
de-prj
├── ELT/
│   ├── consumers/
│   │   └── KafkaToHDFSConsumer.py
│   ├── scrapers/
│   │   ├── LowyatScraper.py
│   │   ├── MarketDataScraper.py
│   │   ├── MarketScraper.py
│   │   ├── RedditScraperManager.py
│   │   └── RedditScraperWorker.py
│   └── utils/
│       ├── CompanyMatcher.py
│       ├── ConsumerBuilder.py
│       ├── ProducerBuilder.py
│       └── SparkBuilder.py
├── data/
│   ├── processed_data/
│   │   ├── lowyat/ (comment.parquet, post.parquet)
│   │   ├── market/ (top_markets.parquet)
│   │   ├── market_data/ (market_data.parquet)
│   │   └── reddit/ (comment.parquet, post.parquet)
│   └── raw_data/
│       ├── lowyat/ (comment.parquet, post.parquet)
│       ├── market/ (top_markets.parquet)
│       ├── market_data/ (market_data.parquet)
│       └── reddit/ (comment.parquet, post.parquet)
├── graph/
│   ├── t4config.py
│   ├── t4nodes.py
│   ├── t4queries.py
│   ├── t4rels.py
│   ├── t4schema.py
│   ├── t4spark.py
│   ├── t4utils.py
│   └── t4visualisation.py
├── mongo/
│   ├── mongodb_ddl.py
│   ├── mongodb_dml_company.py
│   ├── mongodb_dml_market.py
│   ├── mongodb_dml_sentiment.py
│   └── mongodb_utils.py
├── preprocessing/
│   ├── data_cleaner.py
│   ├── data_loader.py
│   ├── data_transformer.py
│   ├── data_validator.py
│   ├── data_writer.py
│   ├── sentiment_analyzer.py
│   ├── spark_session.py
│   └── text_preprocessor.py
├── mock_producers.py
├── mongo_dashboard.py
├── neo4j_dashboard.py
├── spark_dashboard.py
├── streamlit_app.py
├── struct_streaming.py
├── task1.py
├── task2.py
├── task3.py
├── task4.py
├── requirements.txt
└── readme.txt

-----------------------------------------------------------
3. Setup Instructions:
-----------------------------------------------------------

1. Install dependencies:
   $ pip install -r requirements.txt

2. Start required services (as user `hduser`):
   - HDFS (DFS)
   - YARN
   - Zookeeper
   - Kafka

-----------------------------------------------------------
4. Running Tasks:
-----------------------------------------------------------

Task 1
------
Run the Python script for Task 1:
   $ python task1.py

Task 2
------
Run Task 2 using Spark:
   $ spark-submit --py-files preprocessing.zip task2.py

Task 3
------
Run the Python script for Task 3:
   $ python task3.py

Task 4
------
Run the Python script for Task 4:
   $ python task4.py

Task 5 
------
Task 5 is a streaming application.
The `spark_dashboard.py` script will automatically launch the required modules.  
No manual launching of Task 5 is needed.

-----------------------------------------------------------
5. Dashboard:
-----------------------------------------------------------
To view the integrated dashboard, run:
   $ streamlit run streamlit_app.py

This will open a web-based dashboard showing the status and results of all tasks.

-----------------------------------------------------------
6. Notes:
-----------------------------------------------------------
- Always ensure that all required services (HDFS, YARN, Zookeeper, Kafka) are running before executing tasks, especially for streaming applications.
- If Kafka topics are not available, recreate them before running producers.
- Parquet data files in /data are partitioned; only directory names are shown in this README for clarity.