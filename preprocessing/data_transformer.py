from pyspark.sql.functions import from_unixtime, date_format, col, regexp_replace, to_timestamp, split, trim, to_date, monotonically_increasing_id, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
import math

# Author: Lee Chia Chia

class DataTransformer:
    @staticmethod
    def unix_to_date_time(df, timestamp_col, date_fmt="dd-MM-yyyy", time_fmt="HH:mm:ss", drop_cols=None):
        df = df.withColumn("timestamp_tmp", from_unixtime(col(timestamp_col))) \
               .withColumn("Date", date_format(col("timestamp_tmp"), date_fmt)) \
               .withColumn("Time", date_format(col("timestamp_tmp"), time_fmt))
        if drop_cols:
            df = df.drop(*drop_cols)
        return df.drop("timestamp_tmp")

    @staticmethod
    def convert_lowyat_post_dates(df):
        return df.withColumn("clean_date", regexp_replace("last_action", "(\\d{1,2})(st|nd|rd|th)", "\\1")) \
                 .withColumn("timestamp", to_timestamp("clean_date", "d MMMM yyyy - hh:mm a")) \
                 .withColumn("Date", date_format(col("timestamp"), "dd-MM-yyyy")) \
                 .withColumn("Time", date_format(col("timestamp"), "HH:mm:ss")) \
                 .drop("clean_date", "timestamp", "last_action")

    @staticmethod
    def convert_lowyat_comment_dates(df):
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("time", StringType(), True),
        ])
        def extract_transform_date_time(x):
            return (x[0], x[1])
        extract_udf = udf(extract_transform_date_time, schema)
        return df.withColumn("splitted_timestamp", split(col("timestamp"), ",")) \
                 .withColumn("date_time_struct", extract_udf("splitted_timestamp")) \
                 .withColumn("Date", date_format(to_date(trim(col("date_time_struct.date")), "MMM d yyyy"), "dd-MM-yyyy")) \
                 .withColumn("Time", date_format(to_timestamp(trim(col("date_time_struct.time")), "hh:mm a"), "HH:mm:ss")) \
                 .drop("date_time_struct", "timestamp", "splitted_timestamp")

    @staticmethod
    def convert_views_to_int(df):
        def word_convert_digit(value):
            if value.endswith('k'):
                number = float(value[:-1]) * 1000
            elif value.endswith('m'):
                number = float(value[:-1]) * 1_000_000
            else:
                number = float(value)
            return int(number)
        digit_converter = udf(word_convert_digit, IntegerType())
        return df.withColumn("views", digit_converter(col("views")))

    @staticmethod
    def add_id_column(df):
        return df.withColumn("id", monotonically_increasing_id())

    @staticmethod
    def rename_columns(df, mapping):
        for old, new in mapping.items():
            df = df.withColumnRenamed(old, new)
        return df

    @staticmethod
    def normalize_score(df, column_name):
        df = df.withColumn(
                        column_name,
                        (2 / math.pi) * F.atan(F.col(column_name))
                    )
        return df
        
    @staticmethod
    def join_with_post_id(comment_df, post_df, join_col="thread_id", post_id_col="id"):
        return comment_df.join(post_df.select(join_col, post_id_col), on=join_col, how="left")

    @staticmethod
    def format_existing_date(df, col_name="Date"):
        return df.withColumn(col_name, date_format(to_date(col(col_name),"yyyy-MM-dd"), "dd-MM-yyyy"))