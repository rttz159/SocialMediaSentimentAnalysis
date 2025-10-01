import os
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from mongo.mongodb_utils import PyMongoUtils

# Author: Tan Zi Yang

DB_NAME = "DE"

class SentimentDML:
    def __init__(self, db_name: str = DB_NAME):
        self.db = PyMongoUtils().get_database(db_name)
        self.COL = "Sentiment_Daily"

        os.environ["PYSPARK_PYTHON"] = "de-venv/bin/python"
        self.spark = SparkSession.builder.appName("SentimentDML_v2").getOrCreate()

        self.reddit_post_path    = "hdfs://localhost:9000/user/student/processed_data/reddit/post.parquet"
        self.reddit_comment_path = "hdfs://localhost:9000/user/student/processed_data/reddit/comment.parquet"
        self.lowyat_post_path    = "hdfs://localhost:9000/user/student/processed_data/lowyat/post.parquet"
        self.lowyat_comment_path = "hdfs://localhost:9000/user/student/processed_data/lowyat/comment.parquet"
        self.top_market_path     = "hdfs://localhost:9000/user/student/processed_data/market/top_markets.parquet"

    def _company_map(self):
        df = (self.spark.read.parquet(self.top_market_path)
              .select(
                  F.col("Company").alias("CompanyName"),
                  F.col("Stock Code").cast("string").alias("StockCode")
              )
              .dropDuplicates(["StockCode", "CompanyName"])
             )
        return df.select(
            F.lower(F.trim(F.regexp_replace("CompanyName", r"\s+", " "))).alias("name_key"),
            "CompanyName",
            "StockCode"
        )

    def _platform_daily(self, post_path: str, comment_path: str, platform_label: str):

        name_map = self._company_map()

        post_raw = (self.spark.read.parquet(post_path)
                    .withColumn("Date", F.to_date("Date", "dd-MM-yyyy"))
                    .withColumn("CompanyName", F.explode("matched_companies"))
                   )

        post_mapped = (post_raw
            .withColumn("name_key", F.lower(F.trim(F.regexp_replace("CompanyName", r"\s+", " "))))
            .join(name_map, on="name_key", how="inner")
            .select(
                F.col("id").alias("PostID"),
                "Date",
                "StockCode",
                F.col("pos").alias("post_pos"),
                F.col("neg").alias("post_neg"),
                F.col("neu").alias("post_neu")
            )
        )

        comment_raw = (self.spark.read.parquet(comment_path)
                       .withColumn("Date", F.to_date("Date", "dd-MM-yyyy"))
                      )
        comment_agg = (comment_raw
            .groupBy(F.col("post_id").alias("PostID"))
            .agg(
                F.sum("pos").alias("sum_c_pos"),
                F.sum("neg").alias("sum_c_neg"),
                F.sum("neu").alias("sum_c_neu"),
                F.countDistinct("comment_id").alias("numComments")
            )
            .withColumn("numComments", F.col("numComments").cast("long"))
        )

        post_with_comments = (
            post_mapped.alias("p")
            .join(comment_agg.alias("c"), on="PostID", how="left")
            .select(
                F.col("p.PostID").alias("PostID"),
                F.col("p.Date").alias("Date"),
                F.col("p.StockCode").alias("StockCode"),
                F.col("p.post_pos").alias("post_pos"),
                F.col("p.post_neg").alias("post_neg"),
                F.col("p.post_neu").alias("post_neu"),
                F.col("c.sum_c_pos").alias("sum_c_pos"),
                F.col("c.sum_c_neg").alias("sum_c_neg"),
                F.col("c.sum_c_neu").alias("sum_c_neu"),
                F.col("c.numComments").alias("numComments")
            )
            .na.fill({"sum_c_pos": 0.0, "sum_c_neg": 0.0, "sum_c_neu": 0.0, "numComments": 0})
            .withColumn("sum_pos_total", F.col("post_pos") + F.col("sum_c_pos"))
            .withColumn("sum_neg_total", F.col("post_neg") + F.col("sum_c_neg"))
            .withColumn("sum_neu_total", F.col("post_neu") + F.col("sum_c_neu"))
            .withColumn("n_total", F.lit(1) + F.col("numComments"))
            .withColumn("avgPos_post", F.col("sum_pos_total") / F.col("n_total"))
            .withColumn("avgNeg_post", F.col("sum_neg_total") / F.col("n_total"))
            .withColumn("avgNeu_post", F.col("sum_neu_total") / F.col("n_total"))
        )

        daily = (
            post_with_comments
            .groupBy("StockCode", "Date")
            .agg(
                F.sum("sum_pos_total").alias("day_sum_pos"),
                F.sum("sum_neg_total").alias("day_sum_neg"),
                F.sum("sum_neu_total").alias("day_sum_neu"),
                F.sum("n_total").alias("day_n_total"),         
                F.countDistinct("PostID").alias("numPosts"),
                F.sum("numComments").alias("numComments")
            )
            .withColumn("avgPos", F.when(F.col("day_n_total") > 0, F.col("day_sum_pos") / F.col("day_n_total")))
            .withColumn("avgNeg", F.when(F.col("day_n_total") > 0, F.col("day_sum_neg") / F.col("day_n_total")))
            .withColumn("avgNeu", F.when(F.col("day_n_total") > 0, F.col("day_sum_neu") / F.col("day_n_total")))
            .withColumn("content_count", F.col("day_n_total"))
            .select("StockCode", "Date", "avgPos", "avgNeg", "avgNeu", "numPosts", "numComments", "content_count")
        )

        return daily.withColumn("platform", F.lit(platform_label))

    def load_sentiment(self):
        reddit_daily = self._platform_daily(self.reddit_post_path, self.reddit_comment_path, "reddit")
        lowyat_daily = self._platform_daily(self.lowyat_post_path, self.lowyat_comment_path, "lowyat")

        r = (reddit_daily
             .select("StockCode","Date","avgPos","avgNeg","numPosts","numComments","content_count")
             .withColumnRenamed("avgPos","r_pos")
             .withColumnRenamed("avgNeg","r_neg")
             .withColumnRenamed("numPosts","r_posts")
             .withColumnRenamed("numComments","r_comments")
             .withColumnRenamed("content_count","r_weight")
        )

        l = (lowyat_daily
             .select("StockCode","Date","avgPos","avgNeg","numPosts","numComments","content_count")
             .withColumnRenamed("avgPos","l_pos")
             .withColumnRenamed("avgNeg","l_neg")
             .withColumnRenamed("numPosts","l_posts")
             .withColumnRenamed("numComments","l_comments")
             .withColumnRenamed("content_count","l_weight")
        )

        joined = r.join(l, on=["StockCode","Date"], how="outer").na.fill({"r_weight":0,"l_weight":0})

        overall = (joined
            .withColumn("total_w", F.col("r_weight") + F.col("l_weight"))
            .withColumn("overall_pos",
                F.when(F.col("total_w") > 0,
                       (F.coalesce(F.col("r_pos"), F.lit(0.0)) * F.col("r_weight") +
                        F.coalesce(F.col("l_pos"), F.lit(0.0)) * F.col("l_weight")) / F.col("total_w"))
                 .otherwise(F.coalesce(F.col("r_pos"), F.col("l_pos")))
            )
            .withColumn("overall_neg",
                F.when(F.col("total_w") > 0,
                       (F.coalesce(F.col("r_neg"), F.lit(0.0)) * F.col("r_weight") +
                        F.coalesce(F.col("l_neg"), F.lit(0.0)) * F.col("l_weight")) / F.col("total_w"))
                 .otherwise(F.coalesce(F.col("r_neg"), F.col("l_neg")))
            )
            .withColumn("redditNet",
                        F.when(F.col("r_pos").isNotNull() & F.col("r_neg").isNotNull(), F.col("r_pos") - F.col("r_neg")))
            .withColumn("lowyatNet",
                        F.when(F.col("l_pos").isNotNull() & F.col("l_neg").isNotNull(), F.col("l_pos") - F.col("l_neg")))
            .withColumn("net",
                        F.when(F.col("overall_pos").isNotNull() & F.col("overall_neg").isNotNull(),
                               F.col("overall_pos") - F.col("overall_neg")))
            .withColumn("reddit", F.struct(
                F.col("r_pos").alias("avgPos"),
                F.col("r_neg").alias("avgNeg"),
                F.col("r_posts").alias("numPosts"),
                F.col("r_comments").alias("numComments")
            ))
            .withColumn("lowyat", F.struct(
                F.col("l_pos").alias("avgPos"),
                F.col("l_neg").alias("avgNeg"),
                F.col("l_posts").alias("numPosts"),
                F.col("l_comments").alias("numComments")
            ))
            .withColumn("overall", F.struct(
                F.col("overall_pos").alias("avgPos"),
                F.col("overall_neg").alias("avgNeg"),
                F.col("redditNet"),
                F.col("lowyatNet"),
                F.col("net")
            ))
            .select("StockCode","Date","reddit","lowyat","overall")
        )

        docs = []
        for row in overall.collect():
            docs.append({
                "_id": f"{row['StockCode']}_{row['Date'].strftime('%Y-%m-%d')}",
                "StockCode": row["StockCode"],
                "Date": datetime.strptime(str(row["Date"]), "%Y-%m-%d"),
                "reddit": (row["reddit"].asDict() if row["reddit"] is not None else
                           {"avgPos": None, "avgNeg": None, "numPosts": 0, "numComments": 0}),
                "lowyat": (row["lowyat"].asDict() if row["lowyat"] is not None else
                           {"avgPos": None, "avgNeg": None, "numPosts": 0, "numComments": 0}),
                "overall": row["overall"].asDict()
            })
        return docs

    def upsert_many(self, docs):
        for d in docs:
            self.db[self.COL].update_one({"_id": d["_id"]}, {"$set": d}, upsert=True)
        print(f"Upserted {len(docs)} records into '{self.COL}'.")

    def run(self):
        docs = self.load_sentiment()
        self.upsert_many(docs)

if __name__ == "__main__":
    SentimentDML().run()
