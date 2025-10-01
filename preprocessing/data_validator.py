# Author: Lee Chia Chia

class DataValidator:
    @staticmethod
    def remove_null_rows(df, df_name):
        total_before = df.count()
        df_cleaned = df.dropna()
        total_after = df_cleaned.count()
        print(f"Removed {total_before - total_after} null-containing rows from {df_name}")
        return df_cleaned

    @staticmethod
    def remove_invalid_row(comment_df, post_df, name):
        before_count = comment_df.count()
        valid_comment_df = comment_df.join(post_df, comment_df.post_id == post_df.id, how='left_semi')
        after_count = valid_comment_df.count()
        print(f"Removed {before_count - after_count} invalid rows from {name}")
        return valid_comment_df
