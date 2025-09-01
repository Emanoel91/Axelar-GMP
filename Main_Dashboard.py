import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
import plotly.express as px
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar GMP!",
    page_icon="https://img.cryptorank.io/coins/axelar1663924228506.png",
    layout="wide"
)

# --- Title with Logo -----------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="display: flex; align-items: center; gap: 15px;">
        <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="axelar Logo" style="width:60px; height:60px;">
        <h1 style="margin: 0;">Axelar GMP!</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Builder Info ---------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="margin-top: 20px; margin-bottom: 20px; font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" style="width:25px; height:25px; border-radius: 50%;">
            <span>Built by: <a href="https://x.com/0xeman_raz" target="_blank">Eman Raz</a></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Snowflake Connection ----------------------------------------------------------------------------------------
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# --- Date Inputs -------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    timeframe = st.selectbox("Select Time Frame", ["week", "month", "day"])

with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2023-01-01"))

with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-08-31"))

st.markdown(
    """
    <div style="background-color:#ff2776; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Overview of GMP</h2>
    </div>
    """,
    unsafe_allow_html=True
)
# --- Row 1 -----------------------------------------------------------------------------------
@st.cache_data
def load_kpi_data(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH overview as (
    WITH axelar_gmp AS (
  
    SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id, 
    'GMP' AS "Service", 
    data:symbol::STRING AS raw_asset

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    )

SELECT created_at, id, user, source_chain, destination_chain,
     "Service", amount, amount_usd, fee

FROM axelar_gmp)

select count(distinct id) as "Total Transactions",
count(distinct user) as "Unique Users",
round(sum(amount_usd)) as "Total Volume",
round(sum(amount_usd)/count(distinct user)) as "Average Volume per User"
from overview
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
df_kpi = load_kpi_data(start_date, end_date)

# --- KPI Row ------------------------------------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric(
    label="Total Transactions",
    value=f"{df_kpi["Total Transactions"][0]:,} Txns"
)

col2.metric(
    label="Unique Users",
    value=f"{df_kpi['UNIQUE_USERS'][0]:,} Txns"
)

col3.metric(
    label="Total Volume",
    value=f"${df_kpi['TOTAL_VOLUME'][0]:,}"
)

col4.metric(
    label="Average Volume per User",
    value=f"${df_kpi['AVERAGE_VOLUME_PER_USER'][0]:,}"
)
