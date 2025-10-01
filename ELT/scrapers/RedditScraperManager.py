from .RedditScraperWorker import RedditScraperWorker
import threading
import time

# Author: Raymond Teng Toh Zi

class RedditScraperManager:
    def __init__(self, reddit_accounts, subreddits, matcher, r, post_topic, comment_topic):
        self.accounts = reddit_accounts
        self.subreddits = subreddits
        self.matcher = matcher
        self.r = r
        self.post_topic = post_topic
        self.comment_topic = comment_topic
        self.threads = []

    def run(self):
        for i in range(len(self.subreddits)):
            worker = RedditScraperWorker(
                self.accounts[i], 
                self.subreddits[i], 
                self.matcher, 
                self.r,
                self.post_topic,
                self.comment_topic
            )
            self.threads.append(worker)
            worker.start()

        for thread in self.threads:
            thread.join()