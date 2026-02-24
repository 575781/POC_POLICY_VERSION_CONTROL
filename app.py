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
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
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
st.sidebar.write("User:", st.session_state["username"])

if st.sidebar.button("Logout"):
    st.session_state.clear()
    st.experimental_rerun()

st.sidebar.header("Menu")

app_mode = st.sidebar.radio(
    "Select Option",
    ["Search Policy", "Analyze Policy Changes"]
)

filters = load_filter_values()

# =================================================
# SEARCH MODE
# =================================================
if app_mode == "Search Policy":

    st.sidebar.header("Search Filters")

    search_text = st.sidebar.text_input("Search Query")
    lob = st.sidebar.selectbox("LOB", filters["LOB"])
    state = st.sidebar.selectbox("State", filters["STATE"])

    version_df = session.sql(f"""
        SELECT DISTINCT VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE LOB='{lob}' AND STATE='{state}'
        ORDER BY VERSION
    """).to_pandas()

    versions = version_df["VERSION"].tolist()
    version = st.sidebar.selectbox("Version", versions)

    if st.sidebar.button("Search"):

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
# ANALYZE POLICY CHANGES
# =================================================
if app_mode == "Analyze Policy Changes":

    st.sidebar.header("Comparison Filters")

    lob = st.sidebar.selectbox("LOB", filters["LOB"])
    state = st.sidebar.selectbox("State", filters["STATE"])

    file_df = session.sql(f"""
        SELECT DISTINCT FILE_NAME
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE LOB='{lob}' AND STATE='{state}'
        ORDER BY FILE_NAME
    """).to_pandas()

    filenames = file_df["FILE_NAME"].tolist()
    selected_file = st.sidebar.selectbox("Policy File", filenames)

    version_df = session.sql(f"""
        SELECT DISTINCT VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE FILE_NAME='{selected_file}'
        ORDER BY VERSION
    """).to_pandas()

    versions = version_df["VERSION"].tolist()

    old_version = st.sidebar.selectbox("Old Version", versions)
    latest_version = sorted(versions)[-1] if versions else None

    st.sidebar.write("Latest Version:", latest_version)

    def get_doc_id(file_name, version):
        df = session.sql(f"""
            SELECT DOC_ID
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
            WHERE FILE_NAME='{file_name}'
            AND VERSION='{version}'
        """).to_pandas()

        return df.iloc[0]["DOC_ID"] if not df.empty else None

    old_doc_id = get_doc_id(selected_file, old_version)
    new_doc_id = get_doc_id(selected_file, latest_version)

    analyze_btn = st.sidebar.button("Analyze Policy Impact")

    if analyze_btn:

        if not old_doc_id or not new_doc_id:
            st.error("Invalid document selection.")
            st.stop()

        # 🔹 Display DOC IDs (Small & Clean)
        st.markdown(
            f"""
            <div style='font-size:13px;color:gray'>
            Old DOC_ID: <b>{old_doc_id}</b> &nbsp;&nbsp; |
            New DOC_ID: <b>{new_doc_id}</b>
            </div>
            """,
            unsafe_allow_html=True
        )

        # 1️⃣ Generate Clause Diff
        session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.COMPARE_POLICY_VERSIONS(
                {old_doc_id},
                {new_doc_id}
            )
        """).collect()

        # 2️⃣ Generate Summary
        summary_result = session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.GENERATE_CHANGE_SUMMARY(
                {old_doc_id},
                {new_doc_id}
            )
        """).collect()

        summary_json = summary_result[0][0]

        if isinstance(summary_json, str):
            summary_json = json.loads(summary_json)

        # ------------------------------
        # Change Summary Block
        # ------------------------------
        st.markdown("### Change Summary")

        diff_df = session.sql(f"""
            SELECT CHANGE_TYPE, OLD_CLAUSE, NEW_CLAUSE
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_VERSION_DIFFS
            WHERE OLD_DOC_ID={old_doc_id}
            AND NEW_DOC_ID={new_doc_id}
        """).to_pandas()

        col1, col2 = st.columns(2)
        col1.markdown(f"**Old ({old_version})**")
        col2.markdown(f"**New ({latest_version})**")

        for _, row in diff_df.iterrows():

            change = row["CHANGE_TYPE"].lower()
            old_clause = row["OLD_CLAUSE"]
            new_clause = row["NEW_CLAUSE"]

            col1, col2 = st.columns(2)

            if change == "removed":
                col1.markdown(
                    f"<div style='background:#ffe6e6;padding:8px;border-radius:5px'>{old_clause}</div>",
                    unsafe_allow_html=True
                )

            elif change == "added":
                col2.markdown(
                    f"<div style='background:#e6ffe6;padding:8px;border-radius:5px'>{new_clause}</div>",
                    unsafe_allow_html=True
                )

            elif change == "modified":
                col1.markdown(
                    f"<div style='background:#fff8dc;padding:8px;border-radius:5px'>{old_clause}</div>",
                    unsafe_allow_html=True
                )
                col2.markdown(
                    f"<div style='background:#fff8dc;padding:8px;border-radius:5px'>{new_clause}</div>",
                    unsafe_allow_html=True
                )

        # ------------------------------
        # Comparison Block
        # ------------------------------
        st.markdown("### Comparison")

        st.info(summary_json.get("summary", "No summary generated."))

        risks = summary_json.get("risk_highlights", [])

        if risks:
            st.markdown("**Risk Highlights**")
            for risk in risks:
                st.markdown(f"- {risk}")

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
