import streamlit as st
import psycopg2
import pandas as pd
from streamlit import secrets
import google.generativeai as genai

# ---------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------
st.set_page_config(page_title="EAS503 Gemini SQL Assistant", layout="wide")

# ---------------------------------------------------------
# PASSWORD PROTECTION
# ---------------------------------------------------------
PASSWORD = secrets["APP_PASSWORD"]

entered_pw = st.text_input("🔑 Enter password:", type="password")

if entered_pw and entered_pw != PASSWORD:
    st.error("❌ Wrong password. Try again.")

if entered_pw != PASSWORD:
    st.stop()

st.success("✔ Password accepted! Loading dashboard...")


# ---------------------------------------------------------
# CONNECT TO POSTGRES
# ---------------------------------------------------------
@st.cache_resource
def connect_db():
    return psycopg2.connect(
        host=secrets["DB_HOST"],
        dbname=secrets["DB_NAME"],
        user=secrets["DB_USER"],
        password=secrets["DB_PASSWORD"],
        port=secrets["DB_PORT"]
    )

conn = connect_db()


def run_query(sql):
    try:
        return pd.read_sql(sql, conn)
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return None


# ---------------------------------------------------------
# CONFIGURE GEMINI
# ---------------------------------------------------------
genai.configure(api_key=secrets["GEMINI_API_KEY"])
model = genai.GenerativeModel("gemini-2.5-flash")

SYSTEM_PROMPT = """
You are an expert SQL generator for PostgreSQL.
Generate ONLY a valid SQL SELECT query.

IMPORTANT SCHEMA RULES:
The table `orders` has ONLY these columns:

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
order_date (DATE)

STRICT RULES:
- The table DOES NOT have: id, order_id, customer_id, product_id OR ANY numeric ID column.
- NEVER use id in ANY query.
- ALWAYS use COUNT(*) instead of COUNT(id).
- Validate column names EXACTLY.
- ALWAYS return a plain SQL SELECT statement.
- NEVER wrap inside ``` ``` blocks.
- NEVER include extra text, only SQL.
"""


def nl_to_sql(prompt):
    """Convert English → SQL using Gemini safely."""
    response = model.generate_content(
        SYSTEM_PROMPT + "\nUser Prompt: " + prompt
    )
    
    sql = response.text.strip()

    # -----------------------------
    # SANITIZE SQL
    # -----------------------------
    sql = sql.replace("```sql", "")
    sql = sql.replace("```", "")
    sql = sql.replace("`", "")
    sql = sql.strip()

    # Remove accidental "sql " prefix
    if sql.lower().startswith("sql "):
        sql = sql[4:].strip()

    if sql.lower().startswith("sql\n"):
        sql = sql[4:].strip()

    # Remove semicolon
    sql = sql.rstrip(";").strip()

    # 🛑 SAFETY PATCH: Fix hallucinated id column
    sql = sql.replace("COUNT(id)", "COUNT(*)")
    sql = sql.replace("count(id)", "COUNT(*)")

    return sql


# ---------------------------------------------------------
# MAIN UI
# ---------------------------------------------------------
st.title("🤖 Gemini-Powered SQL Assistant (EAS503)")
st.write("Ask anything in English — Gemini converts it into SQL and runs it on the PostgreSQL database.")

prompt = st.text_area("💬 Ask a question (e.g., 'show sales by region'): ")

if st.button("Generate SQL & Run"):

    if not prompt.strip():
        st.warning("Please type a question.")
        st.stop()

    # Generate SQL
    with st.spinner("🤖 Generating SQL using Gemini..."):
        sql_query = nl_to_sql(prompt)

    st.subheader("🧠 Generated SQL")
    st.code(sql_query, language="sql")

    # Run SQL
    with st.spinner("📡 Running query on PostgreSQL..."):
        df = run_query(sql_query)

    if df is not None:
        st.success("✔ Query executed successfully!")
        st.dataframe(df)

        # Auto chart
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
        text_cols = df.select_dtypes(include=["object"]).columns

        if len(numeric_cols) > 0 and len(text_cols) > 0:
            try:
                st.subheader("📊 Auto Chart")
                st.bar_chart(df.set_index(text_cols[0])[numeric_cols[0]])
            except:
                st.info("Chart not available for this query.")

