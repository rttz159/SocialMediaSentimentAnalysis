# Author: Lee Chia Chia

class DataWriter:
    @staticmethod
    def write_parquet(df, path, mode="append"):
        df.write.mode(mode).parquet(path)
