from neo4j import GraphDatabase
from pyspark.sql import functions as F
from graph.t4utils import BatchWriter

# Author: LEWIS LIM RONG ZHEN

class NodeIngestor:
    def __init__(self, uri, user, password):
        self.uri = uri
        self.user = user
        self.password = password
        self.writer = BatchWriter(uri, user, password)

    def reset_database(self):
        with GraphDatabase.driver(self.uri, auth=(self.user, self.password)) as drv:
            with drv.session() as s:
                s.run("MATCH (n) DETACH DELETE n")
        print("Database cleared.")

    def prepare_frames(self, market_data_df, reddit_post_df, lowyat_post_df, reddit_comment_df, lowyat_comment_df, top_market_df):
        bars_df = market_data_df.select(
            F.col("Stock Code").cast("string").alias("code"),
            F.date_format(F.to_date(F.col("Date"), "dd-MM-yyyy"), "yyyy-MM-dd").alias("date"),
            F.col("Open").alias("open"),
            F.col("High").alias("high"),
            F.col("Low").alias("low"),
            F.col("Close").alias("close"),
            F.col("Volume").alias("volume")
        ).dropna(subset=["code", "date"])

        rp = reddit_post_df.withColumn("source", F.lit("Reddit"))
        lp = lowyat_post_df.withColumn("source", F.lit("Lowyat"))
        posts_df = rp.unionByName(lp, allowMissingColumns=True)\
            .withColumn("date", F.to_date(F.col("Date"), "dd-MM-yyyy"))\
            .withColumn("time", F.col("Time"))\
            .withColumn("datetime", F.to_timestamp(F.concat_ws(" ", F.col("Date"), F.col("Time")), "dd-MM-yyyy HH:mm:ss"))\
            .withColumn("negative", F.col("neg"))\
            .withColumn("neutral", F.col("neu"))\
            .withColumn("positive", F.col("pos"))\
            .select("id", "author", "subreddit", "date", "time", "datetime", "negative", "neutral", "positive", "score", "matched_companies", "source")

        rc = reddit_comment_df.withColumn("source", F.lit("Reddit"))
        lc = lowyat_comment_df.withColumn("source", F.lit("Lowyat"))
        comments_df = rc.unionByName(lc, allowMissingColumns=True)\
            .withColumn("date", F.to_date(F.col("Date"), "dd-MM-yyyy"))\
            .withColumn("time", F.col("Time"))\
            .withColumn("datetime", F.to_timestamp(F.concat_ws(" ", F.col("Date"), F.col("Time")), "dd-MM-yyyy HH:mm:ss"))\
            .withColumn("negative", F.col("neg"))\
            .withColumn("neutral", F.col("neu"))\
            .withColumn("positive", F.col("pos"))\
            .select("comment_id", "author", "post_id", "subreddit", "date", "time", "datetime", "negative", "neutral", "positive", "source")

        companies_df = top_market_df.select(
            F.trim(F.col("Company")).alias("company_name"),
            F.trim(F.col("Sector")).alias("sector_name"),
            F.col("Stock Code").cast("string").alias("code")
        ).dropna(subset=["company_name", "code"]).dropDuplicates(["code"])

        return {"bars": bars_df, "posts": posts_df, "comments": comments_df, "companies": companies_df}

    def ingest_companies(self, companies_df):
        cypher_companies = ["""
UNWIND $rows AS row
MERGE (c:Company {code: row.code})
  ON CREATE SET c.name = row.company_name
MERGE (s:Sector {name: row.sector_name});
""", """
UNWIND $rows AS row
MATCH (c:Company {code: row.code}), (s:Sector {name: row.sector_name})
MERGE (c)-[:IN_SECTOR]->(s);
"""]
        self.writer.write(companies_df, cypher_companies)

    def ingest_bars(self, bars_df):
        cypher_bars = ["""
UNWIND $rows AS row
WITH row WHERE row.code IS NOT NULL AND row.date IS NOT NULL
MATCH (c:Company {code: row.code})
MERGE (b:StockBar {code: row.code, date: date(row.date)})
  ON CREATE SET 
    b.open   = toFloat(row.open),
    b.high   = toFloat(row.high),
    b.low    = toFloat(row.low),
    b.close  = toFloat(row.close),
    b.volume = toInteger(row.volume);
""", """
UNWIND $rows AS row
WITH row WHERE row.code IS NOT NULL AND row.date IS NOT NULL
MATCH (c:Company {code: row.code}), (b:StockBar {code: row.code, date: date(row.date)})
MERGE (c)-[:HAS_BAR]->(b);
"""]
        self.writer.write(bars_df, cypher_bars)

    def ingest_posts(self, posts_df):
        cypher_posts = ["""
UNWIND $rows AS row
MERGE (p:Post {id: row.id})
  ON CREATE SET
    p.author    = row.author,
    p.subreddit = row.subreddit,
    p.date      = toString(row.date),
    p.time      = row.time,
    p.datetime  = row.datetime,
    p.negative  = row.negative,
    p.neutral   = row.neutral,
    p.positive  = row.positive,
    p.score     = row.score,
    p.matched_companies = row.matched_companies;
""", """
UNWIND $rows AS row
MERGE (a:Author {name: row.author});
"""]
        self.writer.write(posts_df, cypher_posts)

    def ingest_comments(self, comments_df):
        cypher_comments = ["""
UNWIND $rows AS row
MERGE (c:Comment {id: row.comment_id})
  ON CREATE SET
    c.author    = row.author,
    c.post_id   = row.post_id,
    c.subreddit = row.subreddit,
    c.date      = toString(row.date),
    c.time      = row.time,
    c.datetime  = row.datetime,
    c.negative  = row.negative,
    c.neutral   = row.neutral,
    c.positive  = row.positive;
""", """
UNWIND $rows AS row
MERGE (a:Author {name: row.author});
"""]
        self.writer.write(comments_df, cypher_comments)
