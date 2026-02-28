import duckdb as db

# Connect to the persistent DuckDB file
con = db.connect("../../ProjectData.duckdb")

# Create the logical schemas for your Medallion architecture
con.execute("""
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS gold;
""")

# Create tables (or views) INSIDE those specific schemas
con.execute("""
    CREATE TABLE IF NOT EXISTS bronze.reviews_raw (id INTEGER);
    CREATE TABLE IF NOT EXISTS silver.cleaned_data (id INTEGER);
    CREATE TABLE IF NOT EXISTS gold.ml_features (id INTEGER);
""")

df_tables = con.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema IN ('bronze', 'silver', 'gold')
    ORDER BY table_schema
""").fetchdf()

print(df_tables)

con.close()