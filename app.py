import streamlit as st
import psycopg2
import pandas as pd

# ---------------------------------
# 1) STREAMLIT PAGE & PASSWORD
# ---------------------------------
st.set_page_config(page_title="EAS503 Orders Dashboard", layout="wide")

PASSWORD = "eas503"

entered = st.text_input("🔑 Enter password to access dashboard:", type="password")
if entered != PASSWORD:
    st.stop()

st.success("Password accepted! Loading dashboard...")


# ---------------------------------
# 2) DATABASE CONNECTION
# ---------------------------------
DB_HOST = "dpg-d4lpeoeuk2gs738hpbhg-a.ohio-postgres.render.com"
DB_NAME = "eas503db"
DB_USER = "eas503db_user"
DB_PASSWORD = "GBurbd5Gy2CBIbbdFOWPbZe0rhLFRAgF"
DB_PORT = 5432

@st.cache_resource
def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

conn = connect_db()

def run_query(query):
    return pd.read_sql(query, conn)


# ---------------------------------
# 3) PAGE TITLE
# ---------------------------------
st.title("🛒 EAS503 Orders Analytics Dashboard")
st.write("Streamlit app connected to Render PostgreSQL")


# ---------------------------------
# 4) TWO COLUMN LAYOUT (required)
# ---------------------------------
left_col, right_col = st.columns(2)

# ----- LEFT COLUMN → Row Count -----
with left_col:
    st.subheader("📊 Database Overview")
    total_rows = run_query("SELECT COUNT(*) FROM orders;")
    st.metric(label="Total Rows in Orders Table", value=int(total_rows.iloc[0,0]))

# ----- RIGHT COLUMN → Sample Data -----
with right_col:
    st.subheader("🔍 Sample Rows")
    df_sample = run_query("SELECT * FROM orders LIMIT 10;")
    st.dataframe(df_sample)


# ---------------------------------
# 5) REGION CHART
# ---------------------------------
st.header("🌎 Total Orders by Region")

query_region = """
SELECT region, SUM(quantity_ordered) AS total
FROM orders
GROUP BY region
ORDER BY total DESC;
"""

df_region = run_query(query_region)
st.bar_chart(df_region.set_index("region"))


# ---------------------------------
# 6) COUNTRY CHART
# ---------------------------------
st.header("🗺️ Total Orders by Country")

query_country = """
SELECT country, SUM(quantity_ordered) AS total
FROM orders
GROUP BY country
ORDER BY total DESC;
"""

df_country = run_query(query_country)
st.bar_chart(df_country.set_index("country"))


# ---------------------------------
# 7) PRODUCT CATEGORY CHART
# ---------------------------------
st.header("📦 Total Orders by Product Category")

query_category = """
SELECT product_category, SUM(quantity_ordered) AS total
FROM orders
GROUP BY product_category
ORDER BY total DESC;
"""
# ---------------------------------
# 8) ChatGPT / SQL Query Runner
# ---------------------------------
st.header("💬 ChatGPT SQL Query Runner")

st.write("Type any SQL SELECT query and see the results below.")

user_query = st.text_area("Enter a SQL query:", placeholder="SELECT * FROM orders LIMIT 10;")

if st.button("Run Query"):
    try:
        df_q = run_query(user_query)
        st.dataframe(df_q)
    except Exception as e:
        st.error(f"Error running query: {e}")

df_category = run_query(query_category)
st.bar_chart(df_category.set_index("product_category"))


# ---------------------------------
# 8) SHOW FULL DATA (optional)
# ---------------------------------
st.header("📚 Full Dataset (Optional)")
if st.checkbox("Show full orders table"):
    df_full = run_query("SELECT * FROM orders;")
    st.dataframe(df_full)

