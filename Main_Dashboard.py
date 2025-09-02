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
# --- Row 1 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
    value=f"🔗{df_kpi["Total Transactions"][0]:,} Txns"
)

col2.metric(
    label="Unique Users",
    value=f"💼{df_kpi["Unique Users"][0]:,} Wallets"
)

col3.metric(
    label="Total Volume",
    value=f"💲{df_kpi["Total Volume"][0]:,}"
)

col4.metric(
    label="Average Volume per User",
    value=f"💲{df_kpi["Average Volume per User"][0]:,}"
)

# --- Row 2 ------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_time_series_data(timeframe, start_date, end_date):
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

select 
date_trunc('{timeframe}',created_at) as "Date",
count(distinct id) as "Total Transactions",
count(distinct user) as "Unique Users",
round(sum(amount_usd)) as "Total Volume"
from overview
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
order by 1
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
df_ts = load_time_series_data(timeframe, start_date, end_date)
# --- Row 2 charts -------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()

    fig1.add_bar(
        x=df_ts["Date"], 
        y=df_ts["Total Transactions"], 
        name="Total Transactions", 
        yaxis="y1",
        marker_color="blue"
    )

    fig1.add_trace(go.Scatter(
        x=df_ts["Date"], 
        y=df_ts["Unique Users"], 
        name="Unique Users", 
        mode="lines", 
        yaxis="y2",
        line=dict(color="red")
    ))
    fig1.update_layout(
        title="Number of Users and Transactions Over Time",
        yaxis=dict(title="Txns count"),
        yaxis2=dict(title="Wallet count", overlaying="y", side="right"),
        xaxis=dict(title=" "),
        barmode="group",
        legend=dict(
            orientation="h",   
            yanchor="bottom", 
            y=1.05,           
            xanchor="center",  
            x=0.5
        )
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.area(df_ts, x="Date", y="Total Volume", title="Volume Over Time ($USD)")
    fig2.update_layout(
        xaxis_title=" ",
        yaxis_title="$USD",
        template="plotly_white"
    )
    st.plotly_chart(fig2, use_container_width=True)

# --- Row 3 ----------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_quarterly_data(timeframe, start_date, end_date):
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

select 
date_trunc('{timeframe}',created_at) as "Date", case 
when created_at::date >= '2022-01-01' and created_at::date < '2022-03-31' then 'Q1-2022'
when created_at::date >= '2022-04-01' and created_at::date < '2022-06-30' then 'Q2-2022'
when created_at::date >= '2022-07-01' and created_at::date < '2022-09-30' then 'Q3-2022'
when created_at::date >= '2022-10-01' and created_at::date < '2022-12-31' then 'Q4-2022'
when created_at::date >= '2023-01-01' and created_at::date < '2023-03-31' then 'Q1-2023'
when created_at::date >= '2023-04-01' and created_at::date < '2023-06-30' then 'Q2-2023'
when created_at::date >= '2023-07-01' and created_at::date < '2023-09-30' then 'Q3-2023'
when created_at::date >= '2023-10-01' and created_at::date < '2023-12-31' then 'Q4-2023'
when created_at::date >= '2024-01-01' and created_at::date < '2024-03-31' then 'Q1-2024'
when created_at::date >= '2024-04-01' and created_at::date < '2024-06-30' then 'Q2-2024'
when created_at::date >= '2024-07-01' and created_at::date < '2024-09-30' then 'Q3-2024'
when created_at::date >= '2024-10-01' and created_at::date < '2024-12-31' then 'Q4-2024' 
when created_at::date >= '2025-01-01' and created_at::date < '2025-03-31' then 'Q1-2025'
when created_at::date >= '2025-04-01' and created_at::date < '2025-06-30' then 'Q2-2025'
when created_at::date >= '2025-07-01' and created_at::date < '2025-09-30' then 'Q3-2025'
when created_at::date >= '2025-10-01' and created_at::date < '2025-12-31' then 'Q4-2025'
when created_at::date >= '2026-01-01' and created_at::date < '2026-03-31' then 'Q1-2026'
when created_at::date >= '2026-04-01' and created_at::date < '2026-06-30' then 'Q2-2026'
when created_at::date >= '2026-07-01' and created_at::date < '2026-09-30' then 'Q3-2026'
when created_at::date >= '2026-10-01' and created_at::date < '2026-12-31' then 'Q4-2026'
end as "Quarter",
round(sum(amount_usd)) as "Total Volume",
sum("Total Volume") over (partition by "Quarter" order by "Date" asc) as "Cumulative Volume"
from overview
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1, 2
order by 1

    """
    return pd.read_sql(query, conn)
# --- Load Data --------------------------------------------------------------
quarterly_data = load_quarterly_data(timeframe, start_date, end_date)
# --- stacked bar Chart ------------------------------------------------------
fig_stacked = px.bar(
    quarterly_data,
    x="Date",
    y="Cumulative Volume",
    color="Quarter",
    title="Volume per Quarter Over Time (USD)"
)
fig_stacked.update_layout(barmode="stack", yaxis_title="$USD")
st.plotly_chart(fig_stacked, use_container_width=True)

# --- Row 4 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_kpi_data_chains(start_date, end_date):
    
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

select 
count(distinct source_chain) as "Number of Sources",
count(distinct destination_chain) as "Number of Destinations",
round(avg(amount_usd)) as "Average Volume",
round(max(amount_usd)) as "Max Volumme"
from overview
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
df_kpi_chains = load_kpi_data_chains(start_date, end_date)

# --- KPI Row ------------------------------------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric(
    label="Number of Sources",
    value=f"⛓{df_kpi_chains["Number of Sources"][0]:,} Chains"
)

col2.metric(
    label="Number of Destinations",
    value=f"⛓{df_kpi_chains["Number of Destinations"][0]:,} Chains"
)

col3.metric(
    label="Average Volume",
    value=f"💲{df_kpi_chains["Average Volume"][0]:,}"
)

col4.metric(
    label="Max Volumme",
    value=f"💲{df_kpi_chains["Max Volumme"][0]:,}"
)

# --- Row 5 -------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_chain_data_over_time(timeframe, start_date, end_date):
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

select date_trunc('{timeframe}',created_at) as "Date",
count(distinct source_chain) as "Sources",
count(distinct destination_chain) as "Destinations"
from overview
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
order by 1

    """

    return pd.read_sql(query, conn)

@st.cache_data
def load_moving_average_data(timeframe, start_date, end_date):
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

select 
date_trunc('{timeframe}',created_at) as "Date",
round(sum(amount_usd)) as usd_volume,
avg(usd_volume) over (order by "Date" rows between 4 preceding and current row) as "Avg 30 Day Moving",
avg(usd_volume) over (order by "Date" rows between 8 preceding and current row) as "Avg 60 Day Moving",
avg(usd_volume) over (order by "Date" rows between 12 preceding and current row) as "Avg 90 Day Moving"
from overview
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
order by 1

    """

    return pd.read_sql(query, conn)
# --- Load Data ----------------------------------------------------------------------------------------------------
chain_data_over_time = load_chain_data_over_time(timeframe, start_date, end_date)
moving_average_data = load_moving_average_data(timeframe, start_date, end_date)
# ------------------------------------------------------------------------------------------------------------------

col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_trace(
        go.Scatter(
            x=chain_data_over_time["Date"],
            y=chain_data_over_time["Sources"],
            name="Sources",
            mode="lines",
            yaxis="y1"
        )
    )
    
    fig1.add_trace(
        go.Scatter(
            x=chain_data_over_time["Date"],
            y=chain_data_over_time["Destinations"],
            name="Destinations",
            mode="lines",
            yaxis="y1"
        )
    )

    fig1.update_layout(
        title="Number of Active Chains Over Time",
        yaxis=dict(title="Chain count"),
        xaxis=dict(title=" "),
        legend=dict(
            orientation="h",   
            yanchor="bottom", 
            y=1.05,           
            xanchor="center",  
            x=0.5
        )
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_trace(
        go.Scatter(
            x=moving_average_data["Date"],
            y=moving_average_data["Avg 30 Day Moving"],
            name="Avg 30 Day Moving",
            mode="lines",
            yaxis="y1"
        )
    )
    
    fig2.add_trace(
        go.Scatter(
            x=moving_average_data["Date"],
            y=moving_average_data["Avg 60 Day Moving"],
            name="Avg 60 Day Moving",
            mode="lines",
            yaxis="y1"
        )
    )

    fig2.add_trace(
        go.Scatter(
            x=moving_average_data["Date"],
            y=moving_average_data["Avg 90 Day Moving"],
            name="Avg 90 Day Moving",
            mode="lines",
            yaxis="y1"
        )
    )
    
    fig2.update_layout(
        title="Average 30, 60 & 90 Moving Volume Over Time",
        yaxis=dict(title="$USD"),
        xaxis=dict(title=" "),
        legend=dict(
            orientation="h",   
            yanchor="bottom", 
            y=1.05,           
            xanchor="center",  
            x=0.5
        )
    )
    st.plotly_chart(fig2, use_container_width=True)

st.markdown(
    """
    <div style="background-color:#ff2776; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Analysis of Users</h2>
    </div>
    """,
    unsafe_allow_html=True
)
# --- Row 6 --------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_txn_distribution(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_gmp AS (
SELECT data:call.transaction.from::STRING AS user, count(distinct id), case 
when count(distinct id)=1 then '1 Txn'
when count(distinct id)=2 then '2 Txns'
when count(distinct id)>=3 and count(distinct id)<=5 then '3-5 Txns'
when count(distinct id)>=6 and count(distinct id)<=10 then '6-10 Txns'
when count(distinct id)>=11 and count(distinct id)<=15 then '11-15 Txns'
when count(distinct id)>=16 and count(distinct id)<=25 then '16-25 Txns'
when count(distinct id)>=26 and count(distinct id)<=50 then '26-50 Txns'
when count(distinct id)>=51  then '>50 Txns'
end as "Class"
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received'
and created_at::date>='2022-11-01' and created_at::date<='2025-08-31'
group by 1)

select "Class", count(distinct user) as "Number of Users"
From axelar_gmp
GROUP BY 1
ORDER BY 2 desc
    """

    return pd.read_sql(query, conn)

# --- Load Data --------------------------------------------------------------------------------------
txn_distribution = load_txn_distribution(start_date, end_date)
# ----------------------------------------------------------------------------------------------------
bar_fig = px.bar(
    txn_distribution,
    x="Class",
    y="Number of Users",
    title="Breakdown of Users",
    color_discrete_sequence=["#5e67f8"]
)
bar_fig.update_layout(
    xaxis_title=" ",
    yaxis_title="Wallet count",
    bargap=0.2
)

# ---------------------------------------
color_scale = {
    '1 Txn': '#d9fd51',        # lime-ish
    '2 Txns': '#b1f85a',
    '3-5 Txns': '#8be361',
    '6-10 Txns': '#639d55',
    '11-15 Txns': '#4a7c42',
    '16-25 Txns': '#7a4c89',  # purple-ish
    '26-50 Txns': '#cd00fc',
    '>50 Txns': '#fa1d64',
}

fig_donut_volume = px.pie(
    txn_distribution,
    names="Class",
    values="Number of Users",
    title="Share of Users",
    hole=0.5,
    color="Class",
    color_discrete_map=color_scale
)

fig_donut_volume.update_traces(textposition='outside', textinfo='percent+label', pull=[0.05]*len(txn_distribution))
fig_donut_volume.update_layout(showlegend=True, legend=dict(orientation="v", y=0.5, x=1.1))

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(bar_fig, use_container_width=True)

with col2:
    st.plotly_chart(fig_donut_volume, use_container_width=True)

# --- Row 7 --------------------------------------------------------------------------------------------------------
@st.cache_data
def load_new_users_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH overview as (
WITH axelar_gmp AS (
  
  SELECT  
    created_at,
    data:call.transaction.from::STRING AS user
  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    )

SELECT user, min(created_at::date) as first_txn_date
FROM axelar_gmp
group by 1)

select 
date_trunc('{timeframe}',first_txn_date) as "Date",
count(distinct user) as "New Users",
sum("New Users") over (order by "Date") as "Cumulative New Users"
from overview
where first_txn_date::date>='{start_str}' and first_txn_date::date<='{end_str}'
group by 1
order by 1

    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
new_users_data = load_new_users_data(timeframe, start_date, end_date)
# --- Row 3 --------------------------------------------------------------------------------------------------------

fig1 = go.Figure()

fig1.add_trace(go.Bar(
    x=new_users_data["Date"], 
    y=new_users_data["New Users"], 
    name="New Users", 
    yaxis="y1",
    marker_color="blue"
))

fig1.add_trace(go.Scatter(
    x=new_users_data["Date"], 
    y=new_users_data["Cumulative New Users"], 
    name="Cumulative New Users", 
    mode="lines", 
    yaxis="y2",
    line=dict(color="red")
))

fig1.update_layout(
    title="Number of New Users Over Time",
    yaxis=dict(title="Wallet count"),  
    yaxis2=dict(title="Wallet count", overlaying="y", side="right"),  
    xaxis=dict(title=" "),
    barmode="group",
    legend=dict(
        orientation="h",   
        yanchor="bottom", 
        y=1.05,           
        xanchor="center",  
        x=0.5
    )
)
st.plotly_chart(fig1, use_container_width=True)

# --- Row 8 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_kpi_data_new_user(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_gmp AS (
    SELECT  
        created_at::date AS created_date,
        data:call.transaction.from::STRING AS user
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed'
      AND simplified_status = 'received'
),
overview AS (
    SELECT 
        user, 
        MIN(created_date) AS first_txn_date
    FROM axelar_gmp
    GROUP BY user
),
daily_new_users AS (
    SELECT 
        DATE_TRUNC('day', first_txn_date) AS date,
        COUNT(DISTINCT user) AS new_users
    FROM overview
    WHERE first_txn_date BETWEEN '{start_str}' AND '{end_str}'
    GROUP BY 1
),
stats AS (
    SELECT 
        MAX(SUM(new_users)) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
            AS cumulative_new_users,
        ROUND(AVG(new_users)) AS average_daily_new_users
    FROM daily_new_users
)
SELECT DISTINCT cumulative_new_users, average_daily_new_users
FROM stats

    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
kpi_data_new_user = load_kpi_data_new_user(start_date, end_date)

# --- KPI Row ------------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

col1.metric(
    label="Total New Users",
    value=f"👥{kpi_data_new_user["CUMULATIVE_NEW_USERS"][0]:,} Wallets"
)

col2.metric(
    label="Average Daily New Users",
    value=f"💼{kpi_data_new_user["AVERAGE_DAILY_NEW_USERS"][0]:,} Wallets"
)
# --- Row 9 --------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_pie_data_txn(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_gmp AS (
SELECT data:call.transaction.from::STRING AS user, count(distinct id), case 
when count(distinct id)=1 then '1 Txn'
when count(distinct id)=2 then '2 Txns'
when count(distinct id)=3 then '3 Txns'
when count(distinct id)=4 then '4 Txn'
when count(distinct id)=5 then '5 Txns'
when count(distinct id)=6 then '6 Txns'
when count(distinct id)=7 then '7 Txn'
when count(distinct id)=8 then '8 Txns'
when count(distinct id)=9 then '9 Txns'
when count(distinct id)=10 then '10 Txns'
when count(distinct id)>=11  then '>10 Txns'
end as "Number of Txns"
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received'
and created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1)

select "Number of Txns", count(distinct user) as "Number of Users"
From axelar_gmp
GROUP BY 1
ORDER BY 2 desc

    """

    return pd.read_sql(query, conn)

@st.cache_data
def load_pie_data_day(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_gmp AS (
SELECT data:call.transaction.from::STRING AS user, count(distinct created_at::date), case 
when count(distinct created_at::date)=1 then '1 Day'
when count(distinct created_at::date)=2 then '2 Days'
when count(distinct created_at::date)=3 then '3 Days'
when count(distinct created_at::date)=4 then '4 Days'
when count(distinct created_at::date)=5 then '5 Days'
when count(distinct created_at::date)=6 then '6 Days'
when count(distinct created_at::date)=7 then '7 Days'
when count(distinct created_at::date)=8 then '8 Days'
when count(distinct created_at::date)=9 then '9 Days'
when count(distinct created_at::date)=10 then '10 Days'
when count(distinct created_at::date)>=11  then '>10 Days'
end as "#Days of Activity"
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received'
and created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1)

select "#Days of Activity", count(distinct user) as "Number of Users"
From axelar_gmp
GROUP BY 1
ORDER BY 2 desc
    """

    return pd.read_sql(query, conn)

@st.cache_data
def load_pie_data_path(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with overview as (
WITH axelar_gmp AS (
SELECT data:call.transaction.from::STRING AS user, 
lower(data:call.chain::STRING) AS source_chain,
lower(data:call.returnValues.destinationChain::STRING) AS destination_chain
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received'
and created_at::date>='{start_str}' and created_at::date<='{end_str}')

select user, count(distinct (source_chain || '➡' || destination_chain)), case 
when count(distinct (source_chain || '➡' || destination_chain))=1 then '1 Path'
when count(distinct (source_chain || '➡' || destination_chain))=2 then '2 Paths'
when count(distinct (source_chain || '➡' || destination_chain))=3 then '3 Paths'
when count(distinct (source_chain || '➡' || destination_chain))=4 then '4 Paths'
when count(distinct (source_chain || '➡' || destination_chain))=5 then '5 Paths'
when count(distinct (source_chain || '➡' || destination_chain))=6 then '6 Paths'
when count(distinct (source_chain || '➡' || destination_chain))=7 then '7 Paths'
when count(distinct (source_chain || '➡' || destination_chain))=8 then '8 Paths'
when count(distinct (source_chain || '➡' || destination_chain))=9 then '9 Paths'
when count(distinct (source_chain || '➡' || destination_chain))=10 then '10 Paths'
when count(distinct (source_chain || '➡' || destination_chain))>=11  then '>10 Paths'
end as "Number of Paths"
from axelar_gmp 
group by 1)

select "Number of Paths", count(distinct user) as "Number of Users"
from overview
group by 1
order by 2 desc 
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
pie_data_txn = load_pie_data_txn(start_date, end_date)
pie_data_day = load_pie_data_day(start_date, end_date)
pie_data_path = load_pie_data_path(start_date, end_date)
# --- Layout -------------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

# Pie Chart for Txn Distribution
fig1 = px.pie(
    pie_data_txn, 
    values="Number of Users",    
    names="Number of Txns",    
    title="Share of Transaction by Users"
)
fig1.update_traces(textinfo="percent+label", textposition="inside", automargin=True)

# Pie Chart for #Days of Activity
fig2 = px.pie(
    pie_data_day, 
    values="Number of Users",     
    names="#Days of Activity",    
    title="Share of Active Day by Users"
)
fig2.update_traces(textinfo="percent+label", textposition="inside", automargin=True)

# Pie Chart for path
fig3 = px.pie(
    pie_data_path, 
    values="Number of Users",     
    names="Number of Paths",    
    title="Share of Paths by Users"
)
fig3.update_traces(textinfo="percent+label", textposition="inside", automargin=True)

# display charts
col1.plotly_chart(fig1, use_container_width=True)
col2.plotly_chart(fig2, use_container_width=True)
col3.plotly_chart(fig3, use_container_width=True)

st.markdown(
    """
    <div style="background-color:#ff2776; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Heatmap</h2>
    </div>
    """,
    unsafe_allow_html=True
)
# --- Row 10 ------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_heatmap_data(start_date, end_date):
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

select 
DATE_PART('hour', created_at) AS "Hour",
           CASE WHEN DAYOFWEEK(created_at)=0 THEN 7 
                ELSE DAYOFWEEK(created_at) END || ' - ' || DAYNAME(created_at) AS "Day",
count(distinct id) as "Number of Transfers",
round(sum(amount_usd)) as "Volume of Transfers"
from overview
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1, 2
order by 1, 2
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
df_heatmap_data = load_heatmap_data(start_date, end_date)
# --- Row 10 charts -------------------------------------------------------------------------------------------------

col1, col2 = st.columns(2)

with col1:
    
    heatmap_data = df_heatmap_data.pivot_table(index="Day", columns="Hour", values="Number of Transfers", fill_value=0)
    fig_heatmap = px.imshow(heatmap_data, aspect="auto",
                            title="Heatmap of Transactions",
                            labels=dict(x="Hour", y="Day", color="Number of Transfers"))
    st.plotly_chart(fig_heatmap)

with col2:
    
    heatmap_data = df_heatmap_data.pivot_table(index="Day", columns="Hour", values="Volume of Transfers", fill_value=0)
    fig_heatmap = px.imshow(heatmap_data, aspect="auto",
                            title="Heatmap of Volume",
                            labels=dict(x="Hour", y="Day", color="Volume of Transfers"))
    st.plotly_chart(fig_heatmap)

st.markdown(
    """
    <div style="background-color:#ff2776; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Analysis of Routes</h2>
    </div>
    """,
    unsafe_allow_html=True
)
# --- Row 11 ----------------------------------------------------------------------------------------------------------------

@st.cache_data
def load_path_data(start_date, end_date):
    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
      SELECT  
        created_at,
        data:call.chain::STRING AS source_chain,
        data:call.returnValues.destinationChain::STRING AS destination_chain,
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
        AND created_at::date >= '{start_str}'
        AND created_at::date <= '{end_str}'
    )
    SELECT 
      source_chain || '➡' || destination_chain AS path, 
      COUNT(DISTINCT CREATED_AT::DATE) AS "Active Days",
      COUNT(DISTINCT id) AS "Number of Transfers", 
      COUNT(DISTINCT user) AS "Number of Users", 
      round(COUNT(DISTINCT user)/COUNT(DISTINCT CREATED_AT::DATE)) as "Avg Daily Users",
      count(distinct raw_asset) as "#Transferred Tokens",
      ROUND(SUM(amount_usd)) AS "Volume of Transfers USD",
      ROUND(avg(amount_usd)) as "Avg Volume USD",
      ROUND(median(amount_usd)) as "Median Volume USD",
      ROUND(max(amount_usd)) as "Max Volume USD",
      ROUND(sum(amount_usd)/count(distinct created_at::date)) as "Avg Daily Volume USD"
    FROM axelar_service
    GROUP BY 1
    ORDER BY 2 DESC
    """

    return pd.read_sql(query, conn)

# --- Load data ---
df_path = load_path_data(start_date, end_date)

# --- Show table ---
st.subheader("🔀Overview of Cross-Chain Routes")
df_display = df_path.copy()
df_display.index = df_display.index + 1
df_display = df_display.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
st.dataframe(df_display, use_container_width=True)

# --- Row 12 -------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_source_dest_data(start_date, end_date):
    query = f"""
        WITH overview AS (
            WITH axelar_service AS (
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
            FROM axelar_service
        )
        SELECT source_chain AS "Source Chain", 
               destination_chain AS "Destination Chain",
               ROUND(SUM(amount_usd)) AS "Volume (USD)",
               COUNT(DISTINCT id) AS "Number of Transactions"
        FROM overview
        WHERE created_at::date >= '{start_date}' 
          AND created_at::date <= '{end_date}'
          AND amount_usd IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 3 DESC, 4
        
    """
    return pd.read_sql(query, conn)

# Load Data
src_dest_df = load_source_dest_data(start_date, end_date)

# Bubble Chart 1: Volume
fig_vol = px.scatter(
    src_dest_df,
    x="Source Chain",
    y="Destination Chain",
    size="Volume (USD)",
    color="Source Chain",
    hover_data=["Volume (USD)", "Number of Transactions"],
    title="Volume Heatmap Per Route"
)
st.plotly_chart(fig_vol, use_container_width=True)

# Bubble Chart 2: Number of Transactions
fig_txns = px.scatter(
    src_dest_df,
    x="Source Chain",
    y="Destination Chain",
    size="Number of Transactions",
    color="Source Chain",
    hover_data=["Volume (USD)", "Number of Transactions"],
    title="Transactions Heatmap Per Route"
)

st.plotly_chart(fig_txns, use_container_width=True)
