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
    st.experimental_rerun()

# =================================================
# MODE SELECTION (Restored Block)
# =================================================

st.sidebar.header("📂 Menu")

app_mode = st.sidebar.radio(
    "Select Option",
    ["Search Policy", "Analyze Policy Changes"]
)

st.title("📄 Policy & Control Search")

# =================================================
# SEARCH MODE
# =================================================

if app_mode == "Search Policy":

    st.sidebar.header("🔎 Search Filters")

    # LOB
    lob_df = session.sql("""
        SELECT DISTINCT LOB
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        ORDER BY LOB
    """).to_pandas()

    selected_lob = st.sidebar.selectbox("LOB", lob_df["LOB"].dropna().tolist())

    # STATE
    state_df = session.sql(f"""
        SELECT DISTINCT STATE
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE LOB = '{selected_lob}'
        ORDER BY STATE
    """).to_pandas()

    selected_state = st.sidebar.selectbox("State", state_df["STATE"].dropna().tolist())

    # POLICY
    policy_df = session.sql(f"""
        SELECT DISTINCT POLICY_NAME
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE LOB = '{selected_lob}'
        AND STATE = '{selected_state}'
        ORDER BY POLICY_NAME
    """).to_pandas()

    selected_policy = st.sidebar.selectbox("Policy", policy_df["POLICY_NAME"].dropna().tolist())

    # VERSION
    version_df = session.sql(f"""
        SELECT DISTINCT VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE POLICY_NAME = '{selected_policy}'
        ORDER BY VERSION
    """).to_pandas()

    selected_version = st.sidebar.selectbox("Version", version_df["VERSION"].tolist())

    search_text = st.sidebar.text_input("Search Clause")

    if st.sidebar.button("🔍 Search"):

        result_df = session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.SEARCH_POLICY_CLAUSE(
                '{search_text}',
                '{selected_state}',
                '{selected_lob}',
                '{selected_version}'
            )
        """).to_pandas()

        if result_df.empty:
            st.warning("No matching clauses found.")
        else:
            st.dataframe(result_df, use_container_width=True)

# =================================================
# ANALYZE POLICY CHANGES MODE
# =================================================

if app_mode == "Analyze Policy Changes":

    st.title("📄 Policy Version Comparison")

    st.sidebar.header("🧩 Comparison Filters")

    # 1️⃣ LOB
    lob_df = session.sql("""
        SELECT DISTINCT LOB
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        ORDER BY LOB
    """).to_pandas()

    selected_lob = st.sidebar.selectbox("LOB", lob_df["LOB"].dropna().tolist())

    # 2️⃣ STATE
    state_df = session.sql(f"""
        SELECT DISTINCT STATE
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE LOB = '{selected_lob}'
        ORDER BY STATE
    """).to_pandas()

    selected_state = st.sidebar.selectbox("State", state_df["STATE"].dropna().tolist())

    # 3️⃣ POLICY
    policy_df = session.sql(f"""
        SELECT DISTINCT POLICY_NAME
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE LOB = '{selected_lob}'
        AND STATE = '{selected_state}'
        ORDER BY POLICY_NAME
    """).to_pandas()

    selected_policy = st.sidebar.selectbox("Select Policy", policy_df["POLICY_NAME"].dropna().tolist())

    # 4️⃣ FETCH VERSIONS
    version_df = session.sql(f"""
        SELECT VERSION, DOC_ID
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE POLICY_NAME = '{selected_policy}'
        AND LOB = '{selected_lob}'
        AND STATE = '{selected_state}'
        ORDER BY VERSION
    """).to_pandas()

    if len(version_df) < 2:
        st.warning("At least two versions required for comparison.")
        st.stop()

    version_df = version_df.sort_values("VERSION")

    latest_row = version_df.iloc[-1]
    previous_row = version_df.iloc[-2]

    latest_version = latest_row["VERSION"]
    previous_version = previous_row["VERSION"]

    new_doc_id = latest_row["DOC_ID"]
    old_doc_id = previous_row["DOC_ID"]

    st.sidebar.markdown("### 🔎 Auto Comparison")
    st.sidebar.write(f"Previous Version: {previous_version}")
    st.sidebar.write(f"Latest Version: {latest_version}")

    # =================================================
    # ANALYZE BUTTON
    # =================================================

    if st.sidebar.button("Analyze Policy Impact"):

        st.markdown(f"### 📌 Policy: {selected_policy}")
        st.markdown(f"**Previous Version:** {previous_version} (DOC_ID: {old_doc_id})")
        st.markdown(f"**Latest Version:** {latest_version} (DOC_ID: {new_doc_id})")

        # Call comparison procedure
        session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.COMPARE_POLICY_VERSIONS(
                {old_doc_id},
                {new_doc_id}
            )
        """).collect()

        diff_df = session.sql(f"""
            SELECT OLD_CLAUSE AS "Previous Version",
                   NEW_CLAUSE AS "Latest Version"
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_VERSION_DIFFS
            WHERE OLD_DOC_ID = {old_doc_id}
            AND NEW_DOC_ID = {new_doc_id}
        """).to_pandas()

        st.markdown("### 📊 Version Comparison")

        if diff_df.empty:
            st.info("No differences found between selected versions.")
        else:
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

        risks = summary_json.get("risk_highlights", [])
        if risks:
            for risk in risks:
                st.markdown(f"- {risk}")
        else:
            st.write("No risks identified.")

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
