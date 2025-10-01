from preprocessing.spark_session import SparkSessionBuilder
from preprocessing.data_loader import DataLoader
from preprocessing.data_cleaner import DataCleaner
from preprocessing.data_transformer import DataTransformer
from preprocessing.text_preprocessor import TextPreprocessor
from preprocessing.sentiment_analyzer import SentimentAnalyzer
from preprocessing.data_validator import DataValidator
from preprocessing.data_writer import DataWriter

# Author: Lee Chia Chia

PATHS = {
    "reddit_comment": "hdfs://localhost:9000/user/student/raw_data/reddit/comment.parquet",
    "reddit_post": "hdfs://localhost:9000/user/student/raw_data/reddit/post.parquet",
    "lowyat_comment": "hdfs://localhost:9000/user/student/raw_data/lowyat/comment.parquet",
    "lowyat_post": "hdfs://localhost:9000/user/student/raw_data/lowyat/post.parquet",
    "top_market": "hdfs://localhost:9000/user/student/raw_data/market/top_markets.parquet",
    "market_data": "hdfs://localhost:9000/user/student/raw_data/market_data/market_data.parquet",
}

OUT_PATHS = {
    "reddit_comment": "hdfs://localhost:9000/user/student/processed_data/reddit/comment.parquet",
    "reddit_post": "hdfs://localhost:9000/user/student/processed_data/reddit/post.parquet",
    "lowyat_comment": "hdfs://localhost:9000/user/student/processed_data/lowyat/comment.parquet",
    "lowyat_post": "hdfs://localhost:9000/user/student/processed_data/lowyat/post.parquet",
    "top_market": "hdfs://localhost:9000/user/student/processed_data/market/top_markets.parquet",
    "market_data": "hdfs://localhost:9000/user/student/processed_data/market_data/market_data.parquet",
}

def main():
    spark = SparkSessionBuilder.get_session("CleaningPipeline")
    loader = DataLoader(spark)

    reddit_comment_df = loader.load_parquet(PATHS["reddit_comment"])
    reddit_post_df = loader.load_parquet(PATHS["reddit_post"])
    lowyat_comment_df = loader.load_parquet(PATHS["lowyat_comment"])
    lowyat_post_df = loader.load_parquet(PATHS["lowyat_post"])
    top_market_df = loader.load_parquet(PATHS["top_market"])
    market_data_df = loader.load_parquet(PATHS["market_data"])

    reddit_comment_df = DataCleaner.remove_duplicates(reddit_comment_df)
    reddit_comment_df = DataCleaner.drop_nulls(reddit_comment_df)
    reddit_post_df = DataCleaner.remove_duplicates(reddit_post_df)
    reddit_post_df = DataCleaner.drop_nulls(reddit_post_df)
    lowyat_comment_df = DataCleaner.remove_duplicates(lowyat_comment_df)
    lowyat_comment_df = DataCleaner.drop_nulls(lowyat_comment_df)
    lowyat_post_df = DataCleaner.remove_duplicates(lowyat_post_df)
    lowyat_post_df = DataCleaner.drop_nulls(lowyat_post_df)
    top_market_df = DataCleaner.remove_duplicates(top_market_df)
    top_market_df = DataCleaner.drop_nulls(top_market_df)
    market_data_df = DataCleaner.remove_duplicates(market_data_df)
    market_data_df = DataCleaner.drop_nulls(market_data_df)
    
    reddit_comment_df = DataCleaner.filter_short_text(reddit_comment_df, "body")
    lowyat_comment_df = DataCleaner.filter_short_text(lowyat_comment_df, "content")

    lowyat_post_df = DataTransformer.rename_columns(lowyat_post_df, {"thread_starter": "author"})
    reddit_comment_df = DataCleaner.replace_null_author(reddit_comment_df)
    reddit_comment_df = DataCleaner.remove_removed_deleted(reddit_comment_df)
    reddit_post_df = DataCleaner.replace_null_author(reddit_post_df)
    reddit_post_df = DataCleaner.replace_null_author(reddit_post_df)
    
    reddit_comment_df = reddit_comment_df.drop("score", "matched_companies")

    reddit_comment_df = DataTransformer.unix_to_date_time(reddit_comment_df, "created_utc", drop_cols=["created_utc"])
    reddit_post_df = DataTransformer.unix_to_date_time(reddit_post_df, "created_utc", drop_cols=["created_utc", "url"])
    lowyat_post_df = DataTransformer.convert_lowyat_post_dates(lowyat_post_df)
    lowyat_comment_df = DataTransformer.convert_lowyat_comment_dates(lowyat_comment_df)

    lowyat_post_df = DataTransformer.add_id_column(lowyat_post_df)
    lowyat_comment_df = DataTransformer.rename_columns(lowyat_comment_df, {"post_url": "url"})
    lowyat_comment_df.printSchema()
    lowyat_comment_df = DataTransformer.join_with_post_id(lowyat_comment_df, lowyat_post_df, "url")
    lowyat_comment_df = DataTransformer.rename_columns(lowyat_comment_df, {"id": "post_id"})
    lowyat_comment_df = lowyat_comment_df.drop("url")
    lowyat_post_df = lowyat_post_df.drop("url")

    lowyat_post_df = DataTransformer.convert_views_to_int(lowyat_post_df)
    top_market_df = DataTransformer.unix_to_date_time(top_market_df, "Timestamp", drop_cols=["created_utc", "Timestamp", "Constituent Name", "Market Cap (MYR)"])
    market_data_df = DataTransformer.format_existing_date(market_data_df, "Date")

    preprocessing_udf = TextPreprocessor.get_udf()
    reddit_comment_df = reddit_comment_df.withColumn("body", preprocessing_udf("body"))
    reddit_post_df = reddit_post_df.withColumn("selftext", preprocessing_udf("selftext")).withColumn("title", preprocessing_udf("title"))
    lowyat_comment_df = lowyat_comment_df.withColumn("content", preprocessing_udf("content"))
    lowyat_post_df = lowyat_post_df.withColumn("title", preprocessing_udf("title"))

    sentiment_udf = SentimentAnalyzer.get_udf()
    reddit_comment_df = SentimentAnalyzer.apply_sentiment(reddit_comment_df, "body", sentiment_udf)
    reddit_post_df = SentimentAnalyzer.apply_sentiment_on_combined(reddit_post_df, ["selftext", "title"], sentiment_udf)
    lowyat_comment_df = SentimentAnalyzer.apply_sentiment(lowyat_comment_df, "content", sentiment_udf)
    lowyat_post_df = SentimentAnalyzer.apply_sentiment(lowyat_post_df, "title", sentiment_udf)

    lowyat_post_df = DataTransformer.normalize_score(lowyat_post_df, "views")
    reddit_post_df = DataTransformer.normalize_score(reddit_post_df, "score")
    lowyat_post_df = DataTransformer.rename_columns(lowyat_post_df, {"views": "score"})

    reddit_comment_df = DataValidator.remove_null_rows(reddit_comment_df, "reddit_comment_df")
    reddit_post_df = DataValidator.remove_null_rows(reddit_post_df, "reddit_post_df")
    lowyat_comment_df = DataValidator.remove_null_rows(lowyat_comment_df, "lowyat_comment_df")
    lowyat_post_df = DataValidator.remove_null_rows(lowyat_post_df, "lowyat_post_df")
    top_market_df = DataValidator.remove_null_rows(top_market_df, "top_market_df")
    market_data_df = DataValidator.remove_null_rows(market_data_df, "market_data_df")

    reddit_comment_df = DataValidator.remove_invalid_row(reddit_comment_df, reddit_post_df, "reddit_comment_df")
    lowyat_comment_df = DataValidator.remove_invalid_row(lowyat_comment_df, lowyat_post_df, "lowyat_comment_df")

    DataWriter.write_parquet(reddit_comment_df, OUT_PATHS["reddit_comment"])
    DataWriter.write_parquet(reddit_post_df, OUT_PATHS["reddit_post"])
    DataWriter.write_parquet(lowyat_comment_df, OUT_PATHS["lowyat_comment"])
    DataWriter.write_parquet(lowyat_post_df, OUT_PATHS["lowyat_post"])
    DataWriter.write_parquet(top_market_df, OUT_PATHS["top_market"])
    DataWriter.write_parquet(market_data_df, OUT_PATHS["market_data"])

if __name__ == "__main__":
    main()
