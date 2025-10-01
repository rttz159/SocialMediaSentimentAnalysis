from pyspark.sql.functions import col, length

# Author: Lee Chia Chia

class DataCleaner:
    @staticmethod
    def remove_duplicates(df):
        return df.distinct()

    @staticmethod
    def drop_nulls(df):
        return df.na.drop()

    @staticmethod
    def filter_short_text(df, column, min_length=3):
        return df.filter(length(col(column)) > min_length)

    @staticmethod
    def replace_null_author(df):
        from pyspark.sql.functions import when, lit, lower
        return df.withColumn(
            "author",
            when(col("author").isNull(), lit("__unknown_author__"))
            .when(col("author") == "None", lit("__unknown_author__"))
            .otherwise(col("author"))
        )

    @staticmethod
    def remove_removed_deleted(df, column="body"):
        from pyspark.sql.functions import lower
        return df.filter((lower(col(column)) != "[removed]") & (lower(col(column)) != "[deleted]"))
