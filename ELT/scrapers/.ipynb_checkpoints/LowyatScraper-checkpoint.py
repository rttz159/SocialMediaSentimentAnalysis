from ..utils.CompanyMatcher import CompanyMatcher
from ..utils.ProducerBuilder import ProducerBuilder
import pandas as pd
from collections import defaultdict
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from rapidfuzz import fuzz
import time
import re

# Author: Raymond Teng Toh Zi

class LowyatScraper:
    def __init__(self, matcher, max_offset=150, delay=2, max_comments=200):
        self.max_offset = max_offset
        self.max_comments = max_comments
        self.delay = delay
        self.driver = self._init_driver()
        self.matcher = matcher

    def _init_driver(self):
        options = Options()
        options.add_argument("--headless") 
        return webdriver.Chrome(options=options)

    def _get_soup(self, url):
        self.driver.get(url)
        time.sleep(self.delay)
        return BeautifulSoup(self.driver.page_source, "html.parser")

    def _extract_clean_content(self,content_tag):
        for q in content_tag.select("div.quotetop, div.quotemain"):
            q.decompose()
            
        for img in content_tag.find_all("img"):
            img.decompose()
            
        text = content_tag.get_text(separator="\n", strip=True)
        text = re.sub(r"This post has been edited by.*?: .*", "", text)
    
        return text.strip()

    def scrape_and_publish(self, post_topic, comment_topic):
        base_url = "https://forum.lowyat.net/StockExchange/+{}"
    
        with ProducerBuilder() as p:
            for offset in range(0, self.max_offset, 30):
                url = base_url.format(offset)
                self.driver.get(url)
                time.sleep(self.delay)
    
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                table = soup.select_one("div#forum_topic_list table")
                if not table:
                    continue
    
                rows = table.find_all("tr")[1:]
                for row in rows:
                    title_tag = row.select_one('#forum_topic_title a[href^="/topic/"]')
                    if not title_tag:
                        continue
    
                    title = title_tag.text.strip()
                    title_url = "https://forum.lowyat.net" + title_tag['href']
                    matched_companies = self.matcher.match(title)
    
                    if not matched_companies:
                        continue
    
                    post = {
                        "title": title,
                        "url": title_url,
                        "replies": row.select_one('#forum_topic_replies a').text.strip().replace(',', '') if row.select_one('#forum_topic_replies a') else "0",
                        "thread_starter": row.select_one('#forum_topic_ts a').text.strip() if row.select_one('#forum_topic_ts a') else "",
                        "views": row.select_one('#forum_topic_views').get_text(strip=True) if row.select_one('#forum_topic_views') else "",
                        "last_action": row.select_one('#forum_topic_lastaction span.lastaction').contents[0].strip() if row.select_one('#forum_topic_lastaction span.lastaction') else "",
                        "matched_companies": matched_companies
                    }
                    
                    p.produce(post_topic, post)
                    self.scrape_forum_comment(title_url, comment_topic, p)


    def scrape_forum_comment(self, topic_url, comment_topic, producer):
        offset = 0
        comment_count = 0
    
        while comment_count < self.max_comments:
            paginated_url = f"{topic_url}/+{offset}" if offset > 0 else topic_url
            self.driver.get(paginated_url)
            time.sleep(self.delay)
    
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            content_wrap = soup.select_one("div#topic_content")
    
            if not content_wrap:
                break
    
            post_tables = content_wrap.select("table.post_table")
            if not post_tables:
                break
    
            for table in post_tables:
                if comment_count >= self.max_comments:
                    break
    
                comment_id = table.get("id", "").replace("post_", "")
                author_tag = table.select_one("td.post_td_left span.normalname a")
                timestamp_tag = table.select_one("td.post_td_right span.postdetails")
                content_tag = table.select_one("div.postcolor.post_text")
    
                author = author_tag.text.strip() if author_tag else ""
                timestamp = timestamp_tag.text.strip() if timestamp_tag else ""
                content = self._extract_clean_content(content_tag) if content_tag else ""
    
                if content:
                    comment = {
                        "comment_id": comment_id,
                        "post_url": topic_url,
                        "author": author,
                        "timestamp": timestamp,
                        "content": content
                    }
    
                    producer.produce(comment_topic, comment)
                    comment_count += 1
    
            offset += 20

    def close(self):
        self.driver.quit()
        print("Web driver closed.")