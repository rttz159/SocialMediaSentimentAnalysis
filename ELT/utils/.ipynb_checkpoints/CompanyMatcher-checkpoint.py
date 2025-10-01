import redis
import json
import re
from rapidfuzz import fuzz

# Author: Raymond Teng Toh Zi

class CompanyMatcher:
    def __init__(self, redis_key="markets_cons:company_keywords", threshold=80):
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        raw_keywords = self.redis_client.hgetall(redis_key)

        self.company_keywords = {
            company: json.loads(kws)
            for company, kws in raw_keywords.items()
        }
        self.threshold = threshold

    def normalize(self, text):
        text = text.lower()
        text = re.sub(r"\b(sdn bhd|berhad|bhd|group|holdings|corporation|corp|limited|ltd)\b", "", text)
        text = re.sub(r"[^\w\s]", "", text)
        return text.strip()
        
    def match(self, text):
        if not isinstance(text, str):
            return set()
    
        text_norm = self.normalize(text)
        matched = set()
    
        for company, keywords in self.company_keywords.items():
            for keyword in keywords:
                keyword_norm = self.normalize(keyword)
                if re.search(rf"\b{re.escape(keyword_norm)}\b", text_norm):
                    matched.add(company)
                elif fuzz.partial_ratio(keyword_norm, text_norm) >= self.threshold:
                    matched.add(company)
    
        return list(matched)
    
