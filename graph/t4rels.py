from pyspark.sql import functions as F
from graph.t4utils import BatchWriter

# Author: LEWIS LIM RONG ZHEN

class RelationshipIngestor:
    def __init__(self, uri, user, password):
        self.uri = uri
        self.user = user
        self.password = password
        self.writer = BatchWriter(uri, user, password)

    def link_authors_to_posts(self, posts_df):
        cypher = ["""
UNWIND $rows AS row
MATCH (a:Author {name: row.author}), (p:Post {id: row.id})
MERGE (a)-[r:WROTE]->(p)
  ON CREATE SET r.datetime = coalesce(row.datetime, datetime())
  ON MATCH  SET r.datetime = coalesce(r.datetime, row.datetime);
"""]
        self.writer.write(posts_df, cypher)

    def link_authors_to_comments(self, comments_df):
        cypher = ["""
UNWIND $rows AS row
MATCH (a:Author {name: row.author}), (c:Comment {id: row.comment_id})
MERGE (a)-[r:WROTE]->(c)
  ON CREATE SET r.datetime = coalesce(row.datetime, datetime())
  ON MATCH  SET r.datetime = coalesce(r.datetime, row.datetime);
"""]
        self.writer.write(comments_df, cypher)

    def link_comments_to_posts(self, comments_df):
        cypher = ["""
UNWIND $rows AS row
MATCH (c:Comment {id: row.comment_id}), (p:Post {id: row.post_id})
MERGE (c)-[r:ON]->(p)
  ON CREATE SET r.datetime = coalesce(row.datetime, datetime())
  ON MATCH  SET r.datetime = coalesce(r.datetime, row.datetime);
"""]
        self.writer.write(comments_df, cypher)

    def build_post_company_mentions(self, posts_df, companies_df):
        mentions_df = posts_df.withColumn("company_name", F.explode_outer("matched_companies"))\
            .join(companies_df.select(F.col("company_name"), F.col("code")), on="company_name", how="inner")\
            .select("id", F.col("code")).withColumnRenamed("id", "post_id")
        print("Mentions mapping rows:", mentions_df.count())
        return mentions_df

    def link_posts_to_companies(self, mentions_df):
        cypher = ["""
UNWIND $rows AS row
MATCH (p:Post {id: row.post_id})
MATCH (c:Company {code: row.code})
MERGE (p)-[:MENTIONS]->(c);
"""]
        self.writer.write(mentions_df, cypher)

    def create_sources_and_links(self, posts_df, comments_df):
        cypher_sources = ["""
UNWIND $rows AS row
WITH row WHERE row.source IS NOT NULL
MERGE (s:Source {name: row.source});
"""]
        cypher_post_source = ["""
UNWIND $rows AS row
WITH row WHERE row.source IS NOT NULL
MATCH (p:Post {id: row.id})
MERGE (s:Source {name: row.source})
MERGE (p)-[:FROM]->(s);
"""]
        cypher_comment_source = ["""
UNWIND $rows AS row
WITH row WHERE row.source IS NOT NULL
MATCH (c:Comment {id: row.comment_id})
MERGE (s:Source {name: row.source})
MERGE (c)-[:FROM]->(s);
"""]
        self.writer.write(posts_df, cypher_sources)
        self.writer.write(posts_df, cypher_post_source)
        self.writer.write(comments_df, cypher_comment_source)
