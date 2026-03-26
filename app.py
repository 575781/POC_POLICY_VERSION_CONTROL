import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import pandas as pd

# -------------------------------------------------
# Page Configuration
# -------------------------------------------------
st.set_page_config(
    page_title="Policy & Control Search",
    layout="wide"
)

# -------------------------------------------------
# Session Initialization
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
if "username" not in st.session_state:
    st.session_state["username"] = None
if "app_role" not in st.session_state:
    st.session_state["app_role"] = None

session = get_active_session()

# -------------------------------------------------
# ✅ STYLE + HTML TABLE FUNCTION (NO SCROLL)
# -------------------------------------------------
def generate_styled_html(df):

    def get_change_type(row):
        old = str(row["Previous Version"]) if row["Previous Version"] else ""
        new = str(row["Latest Version"]) if row["Latest Version"] else ""

        if old and not new:
            return "Removed"
        elif new and not old:
            return "Added"
        else:
            return "Modified"

    df["Change Type"] = df.apply(get_change_type, axis=1)

    def get_color(change):
        if change == "Added":
            return "#d1fae5"
        elif change == "Removed":
            return "#fee2e2"
        else:
            return "#fef9c3"

    html = """
    <style>
    table {
        width: 100%;
        border-collapse: collapse;
        font-family: Arial;
    }
    th {
        background-color: #1f2937;
        color: white;
        padding: 10px;
        text-align: left;
    }
    td {
        padding: 10px;
        border-bottom: 1px solid #ddd;
        vertical-align: top;
        white-space: pre-wrap;
        word-wrap: break-word;
    }
    </style>
    <table>
    <tr>
        <th>Previous Version</th>
        <th>Latest Version</th>
        <th>Change Type</th>
    </tr>
    """

    for _, row in df.iterrows():
        color = get_color(row["Change Type"])

        html += f"""
        <tr style="background-color:{color}">
            <td>{row['Previous Version'] or ''}</td>
            <td>{row['Latest Version'] or ''}</td>
            <td><b>{row['Change Type']}</b></td>
        </tr>
        """

    html += "</table>"
    return html

# -------------------------------------------------
# Fetch App Role
# -------------------------------------------------
def get_app_role(user_name):
    df = session.sql("""
        SELECT APP_ROLE
        FROM AI_POC_DB.HEALTH_POLICY_POC.APP_USER_ACCESS
        WHERE (
            UPPER(USER_NAME) = UPPER(:1)
            OR UPPER(USER_NAME) = SPLIT(UPPER(:1), '@')[0]
        )
        AND IS_ACTIVE = TRUE
    """, [user_name]).to_pandas()
    return df.iloc[0]["APP_ROLE"] if not df.empty else None

# -------------------------------------------------
# LOGIN
# -------------------------------------------------
if not st.session_state["authenticated"]:

    st.title("🔐 Policy Search Login")

    with st.form("login_form"):
        login_user = st.text_input("Username")
        login_btn = st.form_submit_button("Login")

    if login_btn:
        role = get_app_role(login_user)

        if not role:
            st.error("❌ You are not authorized.")
            st.stop()

        st.session_state["authenticated"] = True
        st.session_state["username"] = login_user
        st.session_state["app_role"] = role

    st.stop()

# -------------------------------------------------
# Sidebar
# -------------------------------------------------
st.sidebar.success("Authenticated")
st.sidebar.write("👤 User:", st.session_state["username"])

if st.sidebar.button("🚪 Logout"):
    st.session_state.clear()
    st.rerun()

st.sidebar.header("📂 Menu")

app_mode = st.sidebar.radio(
    "Select Option",
    ["Search Policy", "Analyze Policy Changes"]
)

# =================================================
# ANALYZE POLICY CHANGES MODE
# =================================================
if app_mode == "Analyze Policy Changes":

    st.title("📄 Policy Version Comparison")

    with st.sidebar:
        st.header("🧩 Comparison Filters")

        lob_df = session.sql("""
            SELECT DISTINCT LOB
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
            ORDER BY LOB
        """).to_pandas()

        selected_lob = st.selectbox("LOB", lob_df["LOB"].dropna().tolist())

        state_df = session.sql("""
            SELECT DISTINCT STATE
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
            WHERE LOB = :1
            ORDER BY STATE
        """, [selected_lob]).to_pandas()

        selected_state = st.selectbox("State", state_df["STATE"].dropna().tolist())

        policy_df = session.sql("""
            SELECT DISTINCT POLICY_NAME
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
            WHERE LOB = :1 AND STATE = :2
            ORDER BY POLICY_NAME
        """, [selected_lob, selected_state]).to_pandas()

        selected_policy = st.selectbox("Select Policy", policy_df["POLICY_NAME"].dropna().tolist())

    version_df = session.sql("""
        SELECT VERSION, DOC_ID
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE POLICY_NAME = :1 AND LOB = :2 AND STATE = :3
        ORDER BY VERSION
    """, [selected_policy, selected_lob, selected_state]).to_pandas()

    version_df = version_df.sort_values("VERSION")

    latest_row = version_df.iloc[-1]
    previous_row = version_df.iloc[-2]

    old_doc_id = int(previous_row["DOC_ID"])
    new_doc_id = int(latest_row["DOC_ID"])

    st.sidebar.markdown("### 🔎 Auto Comparison")
    st.sidebar.write(f"Previous Version: {previous_row['VERSION']}")
    st.sidebar.write(f"Latest Version: {latest_row['VERSION']}")

    if st.sidebar.button("Analyze Policy Impact"):

        session.sql("""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.COMPARE_POLICY_VERSIONS(:1, :2)
        """, [old_doc_id, new_doc_id]).collect()

        diff_df = session.sql("""
            SELECT OLD_CLAUSE AS "Previous Version",
                   NEW_CLAUSE AS "Latest Version"
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_VERSION_DIFFS
            WHERE OLD_DOC_ID = :1 AND NEW_DOC_ID = :2
        """, [old_doc_id, new_doc_id]).to_pandas()

        st.markdown("### 📊 Version Comparison")

        if diff_df.empty:
            st.info("No differences found.")
        else:
            html_table = generate_styled_html(diff_df)
            st.markdown(html_table, unsafe_allow_html=True)

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
