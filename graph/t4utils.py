from neo4j import GraphDatabase

# Author: LEWIS LIM RONG ZHEN

class BatchWriter:
    def __init__(self, uri, user, password):
        self.uri = uri
        self.user = user
        self.password = password

    def write(self, df, cypher_list, batchsize=5000):
        rows = df.collect()
        total = len(rows)
        print(f"Writing {total} rows with batchsize={batchsize} ...")
        with GraphDatabase.driver(self.uri, auth=(self.user, self.password)) as drv:
            with drv.session() as s:
                for i in range(0, total, batchsize):
                    data = [r.asDict() for r in rows[i:i+batchsize]]
                    for cypher in cypher_list:
                        s.run(cypher, {"rows": data})
        print("Done.")
