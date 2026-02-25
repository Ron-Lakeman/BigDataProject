con = db.connect("../../ProjectData.duckdb")

con.execute("""
    CREATE TABLE IF NOT EXISTS bronze.raw_transactions (
        product_id VARCHAR,
        product_parent BIGINT,
        product_title VARCHAR,
        vine VARCHAR,
        verified_purchase VARCHAR,
        review_headline VARCHAR,
        review_body VARCHAR,
        review_date DATE,
        marketplace_id BIGINT,
        product_category_id BIGINT,
        label BOOLEAN,
        _ingested_at TIMESTAMP,
        _source_file VARCHAR,
        _index BIGINT
    )
""")

con.close()