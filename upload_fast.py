import psycopg2

DB_HOST = "dpg-d4lpeoeuk2gs738hpbhg-a.ohio-postgres.render.com"
DB_NAME = "eas503db"
DB_USER = "eas503db_user"
DB_PASSWORD = "GBurbd5Gy2CBIbbdFOWPbZe0rhLFRAgF"
DB_PORT = 5432

conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)
cur = conn.cursor()

print("Dropping & creating table...")
cur.execute("DROP TABLE IF EXISTS orders;")
cur.execute("""
CREATE TABLE orders (
    customer_name TEXT,
    address TEXT,
    city TEXT,
    country TEXT,
    region TEXT,
    product_name TEXT,
    product_category TEXT,
    product_category_description TEXT,
    product_unit_price NUMERIC(10,2),
    quantity_ordered INTEGER,
    order_date DATE
);
""")
conn.commit()

print("Uploading with COPY CSV...")
with open("orders_flat.csv", "r") as f:
    next(f)  # skip header
    cur.copy_expert(
        "COPY orders FROM STDIN WITH CSV QUOTE '\"' DELIMITER ','",
        f
    )

conn.commit()
cur.close()
conn.close()

print("✅ DONE — upload finished!")

