from ..utils.ProducerBuilder import ProducerBuilder
import threading
import time
import praw

# Author: Raymond Teng Toh Zi

class RedditScraperWorker(threading.Thread):
    def __init__(self, app_config, subreddit, matcher, r, post_topic, comment_topic):
        super().__init__()
        self.app_config = app_config
        self.subreddit_name = subreddit
        self.matcher = matcher
        self.r = r
        self.post_topic = post_topic
        self.comment_topic = comment_topic
        self.reddit = praw.Reddit(
            client_id=app_config["client_id"],
            client_secret=app_config["client_secret"],
            user_agent="Reddit Scraper"
        )

    def run(self):
        print(f"Starting scrape for r/{self.subreddit_name}")
        subreddit = self.reddit.subreddit(self.subreddit_name)

        with ProducerBuilder() as p:
            for submission in subreddit.top(limit=200):
                content = f"{submission.title} {submission.selftext}"
                matched_companies = self.matcher.match(content)

                if not matched_companies:
                    print(f"Skipping {submission.id} (no matched companies)")
                    continue

                self.r.sadd("reddit:seen_posts", submission.id)

                post = {
                    "id": submission.id,
                    "title": submission.title,
                    "author": str(submission.author),
                    "selftext": submission.selftext,
                    "created_utc": submission.created_utc,
                    "score": submission.score,
                    "url": submission.url,
                    "matched_companies": matched_companies,
                    "subreddit": self.subreddit_name
                }
                
                p.produce(self.post_topic, post)

                submission.comments.replace_more(limit=0)
                for comment in submission.comments.list()[:200]:
                    comment_data = {
                        "post_id": submission.id,
                        "comment_id": comment.id,
                        "author": str(comment.author),
                        "body": comment.body,
                        "created_utc": comment.created_utc,
                        "score": comment.score,
                        "matched_companies": matched_companies,
                        "subreddit": self.subreddit_name
                    }
                    
                    p.produce(self.comment_topic, comment_data)

                print(f"Finished {submission.id} with {submission.num_comments} comments. Sleeping 2s.")
                time.sleep(2)
