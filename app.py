import streamlit as st
from snowflake.snowpark.context import get_active_session
import json

# -------------------------------------------------
# Page Configuration
# -------------------------------------------------
st.set_page_config(
    page_title="Policy & Control Search",
    layout="wide"
)

# -------------------------------------------------
# SAFE Session State Initialization
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if "username" not in st.session_state:
    st.session_state["username"] = None

if "app_role" not in st.session_state:
    st.session_state["app_role"] = None

session = get_active_session()

# -------------------------------------------------
# Helper: Fetch App Role
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
# Helper: Load Filter Values
# -------------------------------------------------
def load_filter_values():
    df = session.sql("""
        SELECT DISTINCT LOB, STATE
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_CHANGE_SUMMARY
        ORDER BY 1,2
    """).to_pandas()

    return {
        "LOB": sorted(df["LOB"].dropna().unique().tolist()),
        "STATE": sorted(df["STATE"].dropna().unique().tolist())
    }

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
    st.experimental_rerun()

st.sidebar.header("📂 Menu")

app_mode = st.sidebar.radio(
    "Select Option",
    ["Search Policy", "Analyze Policy Changes"]
)

filters = load_filter_values()

# =================================================
# SEARCH MODE
# =================================================
if app_mode == "Search Policy":

    st.sidebar.header("🔎 Search Filters")

    search_text = st.sidebar.text_input("Search Query")
    lob = st.sidebar.selectbox("LOB", filters["LOB"])
    state = st.sidebar.selectbox("State", filters["STATE"])

    version_df = session.sql(f"""
        SELECT DISTINCT VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_CHANGE_SUMMARY
        WHERE LOB = '{lob}'
        AND STATE = '{state}'
        ORDER BY VERSION
    """).to_pandas()

    versions = version_df["VERSION"].tolist()
    version = st.sidebar.selectbox("Version", versions)

    if st.sidebar.button("🔍 Search"):

        results_df = session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.SEARCH_POLICY_CLAUSE(
                '{search_text}',
                '{state}',
                '{lob}',
                '{version}'
            )
        """).to_pandas()

        st.dataframe(results_df)

# =================================================
# ANALYZE POLICY CHANGES (TABULAR)
# =================================================
if app_mode == "Analyze Policy Changes":

    st.title("🔄 Analyze Policy Changes")

    st.sidebar.header("🧩 Comparison Filters")

    compare_lob = st.sidebar.selectbox("LOB", filters["LOB"])
    compare_state = st.sidebar.selectbox("State", filters["STATE"])

    file_df = session.sql(f"""
        SELECT DISTINCT FILE_NAME
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE LOB = '{compare_lob}'
        AND STATE = '{compare_state}'
        ORDER BY FILE_NAME
    """).to_pandas()

    filenames = file_df["FILE_NAME"].tolist()
    selected_file = st.sidebar.selectbox("Policy File Name", filenames)

    version_df = session.sql(f"""
        SELECT DISTINCT VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_CHANGE_SUMMARY
        WHERE FILE_NAME = '{selected_file}'
        ORDER BY VERSION
    """).to_pandas()

    versions = version_df["VERSION"].tolist()

    old_version = st.sidebar.selectbox("Old Version", versions)
    latest_version = sorted(versions)[-1] if versions else None

    def get_doc_id(file_name, version):
        df = session.sql(f"""
            SELECT DOC_ID
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_CHANGE_SUMMARY
            WHERE FILE_NAME = '{file_name}'
            AND VERSION = '{version}'
        """).to_pandas()
        return df.iloc[0]["DOC_ID"] if not df.empty else None

    old_doc_id = get_doc_id(selected_file, old_version)
    new_doc_id = get_doc_id(selected_file, latest_version)

    if st.sidebar.button("Analyze Policy Impact"):

        if not old_doc_id or not new_doc_id:
            st.error("Invalid document selection.")
            st.stop()

        # Display DOC IDs clearly
        st.markdown(f"**Old DOC_ID:** {old_doc_id}")
        st.markdown(f"**New DOC_ID:** {new_doc_id}")

        # Call Diff Procedure
        session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.COMPARE_POLICY_VERSIONS(
                {old_doc_id},
                {new_doc_id}
            )
        """).collect()

        # Fetch diff in table format
        diff_df = session.sql(f"""
            SELECT OLD_CLAUSE, NEW_CLAUSE
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_VERSION_DIFFS
            WHERE OLD_DOC_ID = {old_doc_id}
            AND NEW_DOC_ID = {new_doc_id}
        """).to_pandas()

        st.markdown("### 📊 Version Comparison")

        st.dataframe(diff_df, use_container_width=True)

        # Generate Summary
        summary_result = session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.GENERATE_CHANGE_SUMMARY(
                {old_doc_id},
                {new_doc_id}
            )
        """).collect()

        summary_json = summary_result[0][0]

        if isinstance(summary_json, str):
            summary_json = json.loads(summary_json)

        st.markdown("### 📌 Summary")
        st.info(summary_json.get("summary", "No summary generated."))

        st.markdown("### ⚠ Risk Highlights")
        for risk in summary_json.get("risk_highlights", []):
            st.markdown(f"- {risk}")

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
