import pandas as pd

def make_orders_dataframe(csv_path: str) -> pd.DataFrame:
    raw = pd.read_csv(csv_path, sep="\t")

    rows = []
    for _, rec in raw.iterrows():
        products = rec["ProductName"].split(";")
        cats = rec["ProductCategory"].split(";")
        descs = rec["ProductCategoryDescription"].split(";")
        prices = rec["ProductUnitPrice"].split(";")
        qtys = rec["QuantityOrderded"].split(";")
        dates = rec["OrderDate"].split(";")

        for i in range(len(products)):
            d = dates[i]
            date_sql = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"

            rows.append([
                rec["Name"],
                rec["Address"],
                rec["City"],
                rec["Country"],
                rec["Region"],
                products[i],
                cats[i],
                descs[i],
                float(prices[i]),
                int(qtys[i]),
                date_sql,
            ])
    df = pd.DataFrame(rows, columns=[
        "customer_name","address","city","country","region",
        "product_name","product_category","product_category_description",
        "product_unit_price","quantity_ordered","order_date"
    ])
    return df

df = make_orders_dataframe("data.csv")
df.to_csv("orders_flat.csv", index=False)
print("orders_flat.csv created:", len(df), "rows")
