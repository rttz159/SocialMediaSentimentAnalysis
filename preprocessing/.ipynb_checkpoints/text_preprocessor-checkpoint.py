import re, emoji
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Author: Lee Chia Chia

class TextPreprocessor:
    @staticmethod
    def preprocessing(string):
        string = re.sub(r"\n(?!\n)", " ", string)
        string = re.sub(r"\n+", "\n", string)
        string = re.sub(r" +", " ", string).strip()
        string = re.sub(r"\n+", " ", string)
        re.sub(r"https?://\S+|\S+@\S+", "[link]", string)
        string = emoji.demojize(string)
        return string

    @staticmethod
    def get_udf():
        return udf(TextPreprocessor.preprocessing, StringType())
