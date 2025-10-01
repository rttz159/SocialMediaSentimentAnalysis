from typing import List, Optional
import pandas as pd

# Author: Lewis Lim Rong Zhen

class T4Queries:
    def __init__(self, driver):
        self.driver = driver

    def list_companies(self) -> List[str]:
        q = "MATCH (c:Company) RETURN c.name AS name ORDER BY name"
        with self.driver.session() as s:
            names = [r["name"] for r in s.run(q)]
        return names

    def sentiment_distribution(self, company_name: str, start_date: str, end_date: str,
                               source: Optional[str] = None) -> pd.DataFrame:
        q = """
        // --- Posts mentioning the company ---
        MATCH (p:Post)-[:MENTIONS]->(c:Company),
              (p)-[:FROM]->(s:Source)
        WHERE c.name = $company_name
          AND date(p.date) >= date($start_date)
          AND date(p.date) <= date($end_date)
          AND ($source IS NULL OR s.name = $source)
        RETURN 
           CASE 
             WHEN p.negative >= p.neutral AND p.negative >= p.positive THEN 'Negative'
             WHEN p.neutral  >= p.negative AND p.neutral  >= p.positive THEN 'Neutral'
             ELSE 'Positive'
           END AS sentiment_class, 
           'Post' AS type,
           count(*) AS count

        UNION ALL

        // --- Comments on posts mentioning the company ---
        MATCH (cm:Comment)-[:ON]->(p:Post)-[:MENTIONS]->(c:Company),
              (cm)-[:FROM]->(s:Source)
        WHERE c.name = $company_name
          AND date(cm.date) >= date($start_date)
          AND date(cm.date) <= date($end_date)
          AND ($source IS NULL OR s.name = $source)
        RETURN 
           CASE 
             WHEN cm.negative >= cm.neutral AND cm.negative >= cm.positive THEN 'Negative'
             WHEN cm.neutral  >= cm.negative AND cm.neutral  >= cm.positive THEN 'Neutral'
             ELSE 'Positive'
           END AS sentiment_class, 
           'Comment' AS type,
           count(*) AS count
        """
        params = {
            "company_name": company_name,
            "start_date": start_date,
            "end_date": end_date,
            "source": source
        }
        with self.driver.session() as s:
            data = [r.data() for r in s.run(q, params)]
        return pd.DataFrame(data)

    def stock_vs_social(self, company_name: str, start_date: str, end_date: str,
                        source: Optional[str] = None) -> pd.DataFrame:
        q = """
        // 1. Get stock bars
        MATCH (c:Company {name: $company_name})-[:HAS_BAR]->(b:StockBar)
        WHERE date(b.date) >= date($start_date) AND date(b.date) <= date($end_date)
        WITH c, b

        // 2. Count posts
        OPTIONAL MATCH (p:Post)-[:MENTIONS]->(c)
        WHERE date(p.date) = date(b.date)
          AND ($source IS NULL OR (p)-[:FROM]->(:Source {name:$source}))
        WITH c, b, count(p) AS post_count

        // 3. Count comments
        OPTIONAL MATCH (cm:Comment)-[:ON]->(p2:Post)-[:MENTIONS]->(c)
        WHERE date(cm.date) = date(b.date)
          AND ($source IS NULL OR (cm)-[:FROM]->(:Source {name:$source}))
        WITH b.date AS date, b.close AS close, b.volume AS volume,
             post_count, count(cm) AS comment_count
        RETURN date, close, volume, 
               post_count, comment_count,
               (post_count + comment_count) AS social_activity
        ORDER BY date
        """
        params = {
            "company_name": company_name,
            "start_date": start_date,
            "end_date": end_date,
            "source": source
        }
        with self.driver.session() as s:
            data = [r.data() for r in s.run(q, params)]
        return pd.DataFrame(data)
