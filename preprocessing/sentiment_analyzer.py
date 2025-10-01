from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import udf, col, concat_ws

# Author: Lee Chia Chia

class SentimentAnalyzer:
    @staticmethod
    def sentiment_scores(sentence):
        if sentence is None:
            return (0.0, 0.0, 0.0)
        sid_obj = SentimentIntensityAnalyzer()
        sentiment_dict = sid_obj.polarity_scores(sentence)
        return (
            float(sentiment_dict['neg']),
            float(sentiment_dict['neu']),
            float(sentiment_dict['pos'])
        )

    @staticmethod
    def get_udf():
        scores_schema = StructType([
            StructField("neg", DoubleType(), False),
            StructField("neu", DoubleType(), False),
            StructField("pos", DoubleType(), False),
        ])
        return udf(SentimentAnalyzer.sentiment_scores, scores_schema)

    @staticmethod
    def apply_sentiment(df, column, sentiment_udf):
        df = df.withColumn("sentiment", sentiment_udf(col(column)))
        return df \
            .withColumn("neg", col("sentiment.neg")) \
            .withColumn("neu", col("sentiment.neu")) \
            .withColumn("pos", col("sentiment.pos")) \
            .drop("sentiment", column)

    @staticmethod
    def apply_sentiment_on_combined(df, columns, sentiment_udf):
        df = df.withColumn("sentiment", sentiment_udf(concat_ws(" ", *columns)))
        return df \
            .withColumn("neg", col("sentiment.neg")) \
            .withColumn("neu", col("sentiment.neu")) \
            .withColumn("pos", col("sentiment.pos")) \
            .drop("sentiment", *columns)
