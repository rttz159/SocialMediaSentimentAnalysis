from graph.t4spark import DataLoader
from graph.t4schema import SchemaManager
from graph.t4nodes import NodeIngestor
from graph.t4rels import RelationshipIngestor
from graph.t4config import Config as cfg

# Author: LEWIS LIM RONG ZHEN

def main():
    loader = DataLoader(cfg.PATHS, cfg.ROW_CAP)
    spark = loader.create_session()
    dfs = loader.load_all()
    loader.print_overview(dfs)

    schema = SchemaManager(cfg.NEO4J_URI, cfg.NEO4J_USER, cfg.NEO4J_PASSWORD)
    schema.drop_legacy_author_source_constraint()
    schema.ensure_constraints()

    nodes = NodeIngestor(cfg.NEO4J_URI, cfg.NEO4J_USER, cfg.NEO4J_PASSWORD)
    nodes.reset_database()

    prepared = nodes.prepare_frames(
        market_data_df=dfs["market_data"],
        reddit_post_df=dfs["reddit_post"],
        lowyat_post_df=dfs["lowyat_post"],
        reddit_comment_df=dfs["reddit_comment"],
        lowyat_comment_df=dfs["lowyat_comment"],
        top_market_df=dfs["top_market"],
    )

    nodes.ingest_companies(prepared["companies"])
    nodes.ingest_bars(prepared["bars"])
    nodes.ingest_posts(prepared["posts"])
    nodes.ingest_comments(prepared["comments"])

    rels = RelationshipIngestor(cfg.NEO4J_URI, cfg.NEO4J_USER, cfg.NEO4J_PASSWORD)
    rels.link_authors_to_posts(prepared["posts"])
    rels.link_authors_to_comments(prepared["comments"])
    rels.link_comments_to_posts(prepared["comments"])
    mentions_df = rels.build_post_company_mentions(prepared["posts"], prepared["companies"])
    rels.link_posts_to_companies(mentions_df)
    rels.create_sources_and_links(prepared["posts"], prepared["comments"])

if __name__ == "__main__":
    main()
