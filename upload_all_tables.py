import psycopg2
import pandas as pd

DB_HOST = "dpg-d4lpeoeuk2gs738hpbhg-a.ohio-postgres.render.com"
DB_NAME = "eas503db"
DB_USER = "eas503db_user"
DB_PASSWORD = "GBurbd5Gy2CBIbbdFOWPbZe0rhLFRAgF"
DB_PORT = 5432


def make_orders_dataframe(csv_path: str) -> pd.DataFrame:
    # Your data.csv is TAB separated
    raw = pd.read_csv(csv_path, sep="\t")

    rows = []
    for _, rec in raw.iterrows():
        products = str(rec["ProductName"]).split(";")
        cats = str(rec["ProductCategory"]).split(";")
        descs = str(rec["ProductCategoryDescription"]).split(";")
        prices = str(rec["ProductUnitPrice"]).split(";")
        qtys = str(rec["QuantityOrderded"]).split(";")
        dates = str(rec["OrderDate"]).split(";")

        n = len(products)
        if not all(len(lst) == n for lst in [cats, descs, prices, qtys, dates]):
            raise ValueError("Mismatched list lengths in row for customer "
                             f"{rec['Name']}")

        for i in range(n):
            date_str = dates[i]
            # from '20120814' -> '2012-08-14'
            date_sql = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"

            rows.append(
                {
                    "customer_name": rec["Name"],
                    "address": rec["Address"],
                    "city": rec["City"],
                    "country": rec["Country"],
                    "region": rec["Region"],
                    "product_name": products[i],
                    "product_category": cats[i],
                    "product_category_description": descs[i],
                    "product_unit_price": float(prices[i]),
                    "quantity_ordered": int(qtys[i]),
                    "order_date": date_sql,
                }
            )

    return pd.DataFrame(rows)


def create_orders_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS orders;")
        cur.execute(
            """
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                customer_name TEXT NOT NULL,
                address TEXT NOT NULL,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                region TEXT NOT NULL,
                product_name TEXT NOT NULL,
                product_category TEXT NOT NULL,
                product_category_description TEXT NOT NULL,
                product_unit_price NUMERIC(10,2) NOT NULL,
                quantity_ordered INTEGER NOT NULL,
                order_date DATE NOT NULL
            );
            """
        )
    conn.commit()


def insert_orders(conn, df: pd.DataFrame):
    records = df.to_dict(orient="records")
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO orders (
                customer_name,
                address,
                city,
                country,
                region,
                product_name,
                product_category,
                product_category_description,
                product_unit_price,
                quantity_ordered,
                order_date
            )
            VALUES (
                %(customer_name)s,
                %(address)s,
                %(city)s,
                %(country)s,
                %(region)s,
                %(product_name)s,
                %(product_category)s,
                %(product_category_description)s,
                %(product_unit_price)s,
                %(quantity_ordered)s,
                %(order_date)s
            );
            """,
            records,
        )
    conn.commit()


def main():
    print("Reading CSV and flattening orders…")
    df = make_orders_dataframe("data.csv")
    print(f"Total rows to insert: {len(df)}")

    print("Connecting to Render PostgreSQL…")
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )

    try:
        print("Creating orders table…")
        create_orders_table(conn)

        print("Inserting data (this may take a bit)…")
        insert_orders(conn, df)

        print("✅ Done! orders table created and populated.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
