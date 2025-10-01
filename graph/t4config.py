from neo4j import GraphDatabase

# Author: LEWIS LIM RONG ZHEN

class Config:
    NEO4J_URI = "PLACEHOLDER"
    NEO4J_USER = "PLACEHOLDER"
    NEO4J_PASSWORD = "PLACEHOLDER"
    PATHS = {
        "reddit_comment": "hdfs://localhost:9000/user/student/processed_data/reddit/comment.parquet",
        "reddit_post": "hdfs://localhost:9000/user/student/processed_data/reddit/post.parquet",
        "lowyat_comment": "hdfs://localhost:9000/user/student/processed_data/lowyat/comment.parquet",
        "lowyat_post": "hdfs://localhost:9000/user/student/processed_data/lowyat/post.parquet",
        "top_market": "hdfs://localhost:9000/user/student/processed_data/market/top_markets.parquet",
        "market_data": "hdfs://localhost:9000/user/student/processed_data/market_data/market_data.parquet",
    }
    ROW_CAP = None
    
    @classmethod
    def get_driver(cls):
        return GraphDatabase.driver(cls.NEO4J_URI, auth=(cls.NEO4J_USER, cls.NEO4J_PASSWORD))
