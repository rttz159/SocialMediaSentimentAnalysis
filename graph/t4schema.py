from neo4j import GraphDatabase

# Author: LEWIS LIM RONG ZHEN

class SchemaManager:
    def __init__(self, uri, user, password):
        self.uri = uri
        self.user = user
        self.password = password

    def drop_legacy_author_source_constraint(self):
        q = "SHOW CONSTRAINTS YIELD name, labelsOrTypes, properties RETURN name, labelsOrTypes, properties"
        drops = []
        with GraphDatabase.driver(self.uri, auth=(self.user, self.password)) as drv:
            with drv.session() as s:
                res = s.run(q).data()
                for row in res:
                    labels = row["labelsOrTypes"]
                    props = row["properties"]
                    if labels == ['Author'] and props and 'source' in props:
                        drops.append(row["name"])
                for name in drops:
                    s.run(f"DROP CONSTRAINT {name} IF EXISTS")
                    print(f"Dropped constraint: {name}")
        if not drops:
            print("No legacy Author(name,source) constraints found.")

    def ensure_constraints(self):
        stmts = [
            "MATCH (c:Company) WHERE c.code IS NULL DETACH DELETE c",
            "CREATE CONSTRAINT post_id IF NOT EXISTS FOR (p:Post) REQUIRE p.id IS UNIQUE",
            "CREATE INDEX post_date_idx IF NOT EXISTS FOR (p:Post) ON (p.date)",
            "CREATE INDEX post_time_idx IF NOT EXISTS FOR (p:Post) ON (p.time)",
            "CREATE CONSTRAINT comment_id IF NOT EXISTS FOR (c:Comment) REQUIRE c.id IS UNIQUE",
            "CREATE INDEX comment_date_idx IF NOT EXISTS FOR (c:Comment) ON (c.date)",
            "CREATE INDEX comment_time_idx IF NOT EXISTS FOR (c:Comment) ON (c.time)",
            "CREATE CONSTRAINT author_name IF NOT EXISTS FOR (a:Author) REQUIRE a.name IS UNIQUE",
            "CREATE CONSTRAINT company_code IF NOT EXISTS FOR (c:Company) REQUIRE c.code IS UNIQUE",
            "CREATE INDEX company_name_idx IF NOT EXISTS FOR (c:Company) ON (c.name)",
            "CREATE CONSTRAINT sector_name IF NOT EXISTS FOR (s:Sector) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT stockbar_key IF NOT EXISTS FOR (b:StockBar) REQUIRE (b.code, b.date) IS NODE KEY",
        ]
        with GraphDatabase.driver(self.uri, auth=(self.user, self.password)) as drv:
            with drv.session() as s:
                for q in stmts:
                    s.run(q)
        print("Constraints and indexes ensured.")
