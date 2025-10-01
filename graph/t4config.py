from neo4j import GraphDatabase

# Author: LEWIS LIM RONG ZHEN

class Config:
    NEO4J_URI = "neo4j+s://c59ab6f6.databases.neo4j.io"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "gntkm-_ASiW47o5IxfFXXGD6Jhr-T86r8SUMmOAye4U"
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