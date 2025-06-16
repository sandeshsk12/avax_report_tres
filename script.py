# app.py
import streamlit as st
import pandas as pd
from flipside import Flipside
import streamlit as st

api_key = st.secrets["API_KEY"]

###############################################################################
# ---------------------------  CONFIG & HELPERS  -----------------------------#
###############################################################################

st.set_page_config(layout="wide")
st.title("CSV File Upload and Data-Quality Check")

# --- Generic utilities ------------------------------------------------------#
def flatten(list_of_lists):
    """Turn [[1,2],[3]] → [1,2,3]."""
    return [item for sub in list_of_lists for item in sub]

def col_style():
    """Add one global button style-block only once."""
    st.markdown(
        """
        <style>
            .stButton>button {
                height: 3em; width: 100%;
                background:#ff4b4b; color:white;
                font-size:1.2em; font-weight:bold;
            }
            .stButton>button:hover {background:#ff0000; border-color:#ff0000;}
        </style>
        """,
        unsafe_allow_html=True,
    )

# --- Data-quality helper-functions ------------------------------------------#
def null_check(df, column):
    """Return rows where column is null."""
    return df[df[column].isnull()]

def length_check(df, column, valid_lengths):
    """Return rows whose stripped length is not in valid_lengths."""
    mask = ~df[column].isnull() & ~df[column].str.strip().str.len().isin(valid_lengths)
    return df[mask]

def report_block(label, message, rows, severity="success"):
    """
    Show a Streamlit block with a dataframe if problems exist,
    else show a green tick.
    """
    if rows.empty:
        st.success(f"✅ No **{message}** records in **{label}**")
        return False      # no error/warning
    else:
        msg_map = {
            "error": f"❌ Null value found in **{label}**",
            "warning": f"⚠️ Corrupted value found in **{label}**",
        }
        getattr(st, severity)(msg_map[severity])
        st.dataframe(rows)
        return True       # an issue exists

###############################################################################
# -----------------------------  MAIN LOGIC  ---------------------------------#
###############################################################################

uploaded = st.file_uploader("Choose an XLSX file", type="xlsx")
if not uploaded:
    st.info("👆 Upload an XLSX to begin")
    st.stop()

df = pd.read_excel(uploaded, sheet_name="Ledger")
st.success("XLSX uploaded successfully")
st.dataframe(df.head())

st.markdown("---")
st.subheader("Data-Quality Checks")

# Store problems for later
issue_hashes      = []          # flat list of txn-hashes
issue_rows_df     = pd.DataFrame()
errors_found      = False
warnings_found    = False

# 1️⃣  Belongs To Address ------------------------------------------------------#
null_rows = null_check(df, "Belongs To")
errors_found |= report_block("Belongs To", "Missing", null_rows, severity="error")

corr_rows = length_check(df, "Belongs To", {42})
if report_block("Belongs To", "Corrupted", corr_rows, severity="warning"):
    warnings_found  = True
    issue_rows_df   = pd.concat([issue_rows_df, corr_rows])
    issue_hashes   += corr_rows["Transaction Hash"].tolist()

# 2️⃣  Transaction Hash -------------------------------------------------------#
null_rows = null_check(df, "Transaction Hash")
errors_found |= report_block("Transaction Hash", "Missing", null_rows, severity="error")

corr_rows = length_check(df, "Transaction Hash", {66})
if report_block("Transaction Hash", "Corrupted", corr_rows, severity="warning"):
    warnings_found  = True
    issue_rows_df   = pd.concat([issue_rows_df, corr_rows])
    issue_hashes   += corr_rows["Transaction Hash"].tolist()

# 3️⃣  Asset Address ----------------------------------------------------------#
null_rows = null_check(df, "Asset Address")
if report_block("Asset Address", "Missing", null_rows, severity="warning"):
    warnings_found  = True
    issue_rows_df   = pd.concat([issue_rows_df, null_rows])
    issue_hashes   += null_rows["Transaction Hash"].tolist()

corr_rows = length_check(df, "Asset Address", {0, 6, 42})
if report_block("Asset Address", "Corrupted", corr_rows, severity="warning"):
    warnings_found  = True
    issue_rows_df   = pd.concat([issue_rows_df, corr_rows])
    issue_hashes   += corr_rows["Transaction Hash"].tolist()

# 4️⃣  Balance Impact (T) -----------------------------------------------------#
null_rows = null_check(df, "Balance Impact (T)")
if report_block("Balance Impact (T)", "Missing", null_rows, severity="warning"):
    warnings_found  = True
    issue_rows_df   = pd.concat([issue_rows_df, null_rows])
    issue_hashes   += null_rows["Transaction Hash"].tolist()

###############################################################################
# ------------------  Summary & (optional) Auto-Fix  -------------------------#
###############################################################################

if not errors_found and not warnings_found:
    st.success("🎉 No errors or warnings detected!")
    st.stop()

col_style()  # inject CSS once
# issue_hashes = list(dict.fromkeys(flatten(issue_hashes)))  # de-dup & preserve order

if errors_found:
    st.error(
        "❌ **Errors** detected. "
        "Please correct them locally and re-upload the file."
    )
    st.stop()

    
transactions_with_issues = issue_hashes.copy()
if st.button("Fix it", type="primary"):
    st.write("🔄 Fetching replacement rows from Flipside…")
    sql_condition = " OR ".join([f"tx_hash ILIKE '%{pattern}%'" for pattern in transactions_with_issues])
    flipside = Flipside(api_key, "https://api-v2.flipsidecrypto.xyz")

    sql = """
            with 
            token_transfers as 
            (
            select block_timestamp, block_number, tx_hash, from_address, to_address, contract_address, name, symbol, amount, 'token_transfers' as event from avalanche.core.ez_token_transfers
            where block_timestamp > '2025-04-01'
            and ({})
            ),
            native_transfers as 
            (
            select block_timestamp, block_number, tx_hash, from_address, to_address, 'native' as contract_address, 'native' as name, 'AVAX' as symbol, amount, 'native_transfers' as event from avalanche.core.ez_native_transfers
            where 1=1
            and ({})
            and block_timestamp > '2025-04-01'
            ),
            tx_fees as 
            (
            select block_timestamp, block_number, tx_hash, from_address,'native' as to_address, 'native' as contract_address,'native' as name, 'AVAX' as symbol, tx_fee as amount, 'pay fees' as event  from avalanche.core.fact_transactions
            where 1=1
            and ({})
            and block_timestamp > '2025-04-01' 
            )
            select * from token_transfers
            union all 
            select * from native_transfers 
            union all 
            select * from tx_fees 

    """.format(sql_condition, sql_condition, sql_condition)
        
    # ---------------------------------------------------------------------------
    # 1) Pull results back from Flipside and shove them into a DataFrame
    # ---------------------------------------------------------------------------
    query_result_set = flipside.query(sql)                                   # execute SQL
    fs_df = pd.DataFrame(                                                    # convert → DataFrame
        query_result_set.rows, 
        columns=query_result_set.columns
    )

    # Desired column order for the **final** records list we’re about to build
    imp_df_cols = [
        'Timestamp', 'Belongs To', 'Balance Impact (T)', 'Original Currency Symbol',
        'Asset Verification Status', 'Belongs To Address', 'Direction',
        'From Address', 'To Address', 'Event Label', 'Original Amount',
        'Original Currency Name', 'Currency Group Symbol', 'Contract Address',
        'Asset Address', 'Transaction Hash', 'Block Number'
    ]

    records = []      # will collect all synthetic rows we create below

    # ---------------------------------------------------------------------------
    # 2) Pre-compute lowercase addresses for fast, case-insensitive matching
    # ---------------------------------------------------------------------------
    belong_to_lower = df['Belongs To'].str.lower().values

    # Decision-table describing every “branch” that the old if/elif chain covered.
    # Do NOT change any logic – only documented here for clarity.
    cases = [
        ("to_address",   "token_transfers",  +1, "inflow",  "Recieve Tokens", "unverified", False),
        ("from_address", "token_transfers",  -1, "outflow", "Send Tokens",    "unverified", False),
        ("to_address",   "native_transfers", +1, "inflow",  "Recieve Coins",  "verified",  True),
        ("from_address", "native_transfers", -1, "outflow", "Send Coins",     "verified",  True),
        ("from_address", "pay fees",         -1, "fees",    "Pay Fees",       "verified",  True),
    ]

    # ---------------------------------------------------------------------------
    # 3) Walk every on-chain row (fs_df) and, if it matches a wallet we care about,
    #    create the synthetic “ledger” row in exactly the same order defined above.
    # ---------------------------------------------------------------------------
    for _, row in fs_df.iterrows():
        from_address = row['from_address'].lower()
        to_address   = row['to_address'].lower()

        # iterate through the decision table
        for addr_field, event, sign, direction, label, status, native_flag in cases:
            addr_lower = to_address if addr_field == "to_address" else from_address

            # skip if event / address don’t match the current case
            if (row['event'] != event) or (addr_lower not in belong_to_lower):
                continue

            matched       = df[df['Belongs To'].str.lower() == addr_lower].iloc[0]
            contract_col  = 'native' if native_flag else row['contract_address']

            record = [
                row['block_timestamp'],                 # Timestamp
                matched['Belongs To'],                  # Wallet name
                sign * row['amount'],                   # Balance Impact (T)
                'AVAX',                                 # Original Currency Symbol
                status,                                 # Asset Verification Status
                matched['Belongs To Address'],          # Wallet address (42-char)
                direction,                              # inflow / outflow / fees
                row['from_address'],                    # From Address
                row['to_address'],                      # To Address
                label,                                  # Human-readable event label
                row['amount'],                          # Original Amount
                row['name'],                            # Original Currency Name
                row['symbol'],                          # Currency Group Symbol
                contract_col,                           # Contract Address
                row['contract_address'],                # Asset Address
                row['tx_hash'],                         # Transaction Hash
                row['block_number'],                    # Block Number
            ]
            records.append(record)
            break   # move to next on-chain row once a case matches

    # ---------------------------------------------------------------------------
    # 4) Turn collected records into a DataFrame
    # ---------------------------------------------------------------------------
    output_df = pd.DataFrame(records, columns=imp_df_cols)

    # ---------------------------------------------------------------------------
    # 5) Merge with original CSV after removing rows flagged as problematic
    # ---------------------------------------------------------------------------
    df_without_issues = df[~df['Transaction Hash'].isin(transactions_with_issues)]
    combined_df = pd.concat([df_without_issues, output_df], ignore_index=True)

    # Drop duplicates that share both txn-hash **and** event label; keep the newest
    combined_df = combined_df.drop_duplicates(
        subset=['Belongs To', 'Transaction Hash', 'Event Label'],
        keep='last'
    )

    # ---------------------------------------------------------------------------
    # 6) Pivot to inflow / outflow / fees view per wallet & asset
    # ---------------------------------------------------------------------------
    pivot_df = combined_df.pivot_table(
        index=['Belongs To', 'Asset Address'],
        columns='Direction',
        values='Balance Impact (T)',
        aggfunc='sum'
    )

    # ---------------------------------------------------------------------------
    # 7) Clean up the pivot (abs values, fill NaNs, compute total)
    # ---------------------------------------------------------------------------
    pivot_df_reset = pivot_df.reset_index()
    pivot_df_reset['inflow']  = pivot_df_reset['inflow'].abs()
    pivot_df_reset['outflow'] = pivot_df_reset['outflow'].abs()
    pivot_df_reset['fees']    = pivot_df_reset['fees'].abs()

    pivot_df_reset[['inflow', 'outflow', 'fees']] = pivot_df_reset[
        ['inflow', 'outflow', 'fees']
    ].fillna(0)

    pivot_df_reset['Total (T)'] = (
        pivot_df_reset['inflow'] - pivot_df_reset['outflow'] - pivot_df_reset['fees']
    )

    # Map Asset Address → Symbol for readability
    address_symbol_df = df[['Asset Address', 'Original Currency Symbol']].drop_duplicates()
    pivot_df_reset = pivot_df_reset.merge(address_symbol_df, on='Asset Address', how='left')

    # ---------------------------------------------------------------------------
    # 8) Present to user & offer CSV download
    # ---------------------------------------------------------------------------
    final_df = pivot_df_reset.sort_values(['Belongs To', 'Asset Address'])
    desired_column_order = [
    "Belongs To",
    "Asset Address",
    "Original Currency Symbol",
    "inflow",
    "outflow",
    "fees",
    "Total (T)",
    ]

    final_df = final_df[desired_column_order + [c for c in final_df.columns if c not in desired_column_order]]

    st.dataframe(final_df)  # show in Streamlit
    

    csv = final_df.to_csv(index=False)
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name='processed_transactions.csv',
        mime='text/csv',
        key='download report',
        use_container_width=True,
        help="Click to download the processed data as a CSV file"
    )

    df_balance_summary = pd.read_excel(uploaded, sheet_name="Balance Statement")
    



