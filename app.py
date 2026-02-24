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
# LOGIN SCREEN
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
    ["Analyze Policy Changes"]
)

st.title("📄 Policy Version Comparison")

# =================================================
# ANALYZE POLICY CHANGES (POLICY_NAME BASED)
# =================================================
if app_mode == "Analyze Policy Changes":

    st.sidebar.header("🧩 Comparison Filters")

    # 1️⃣ Fetch Policy Names
    policy_df = session.sql("""
        SELECT DISTINCT POLICY_NAME
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        ORDER BY POLICY_NAME
    """).to_pandas()

    policy_names = policy_df["POLICY_NAME"].tolist()

    selected_policy = st.sidebar.selectbox(
        "Select Policy",
        policy_names
    )

    # 2️⃣ Fetch Versions + DOC_ID for selected policy
    version_df = session.sql(f"""
        SELECT VERSION, DOC_ID
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE POLICY_NAME = '{selected_policy}'
        ORDER BY VERSION
    """).to_pandas()

    if version_df.empty:
        st.warning("No versions found for selected policy.")
        st.stop()

    versions = version_df["VERSION"].tolist()

    old_version = st.sidebar.selectbox("Old Version", versions)
    latest_version = versions[-1]

    # 3️⃣ Extract DOC IDs
    old_doc_id = version_df[
        version_df["VERSION"] == old_version
    ]["DOC_ID"].values[0]

    new_doc_id = version_df[
        version_df["VERSION"] == latest_version
    ]["DOC_ID"].values[0]

    if st.sidebar.button("Analyze Policy Impact"):

        if old_doc_id == new_doc_id:
            st.warning("Old and Latest versions are the same. Please choose different versions.")
            st.stop()

        st.markdown(f"### 📌 Selected Policy: {selected_policy}")
        st.markdown(f"**Old Version:** {old_version} (DOC_ID: {old_doc_id})")
        st.markdown(f"**Latest Version:** {latest_version} (DOC_ID: {new_doc_id})")

        # -------------------------------------------------
        # Call Diff Procedure
        # -------------------------------------------------
        session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.COMPARE_POLICY_VERSIONS(
                {old_doc_id},
                {new_doc_id}
            )
        """).collect()

        # -------------------------------------------------
        # Fetch Diff Table
        # -------------------------------------------------
        diff_df = session.sql(f"""
            SELECT OLD_CLAUSE AS "Old Version",
                   NEW_CLAUSE AS "New Version"
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_VERSION_DIFFS
            WHERE OLD_DOC_ID = {old_doc_id}
            AND NEW_DOC_ID = {new_doc_id}
        """).to_pandas()

        st.markdown("### 📊 Version Comparison (Tabular)")

        if diff_df.empty:
            st.info("No differences found between selected versions.")
        else:
            st.dataframe(diff_df, use_container_width=True)

        # -------------------------------------------------
        # Generate LLM Summary
        # -------------------------------------------------
        summary_result = session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.GENERATE_CHANGE_SUMMARY(
                {old_doc_id},
                {new_doc_id}
            )
        """).collect()

        summary_json = summary_result[0][0]

        if isinstance(summary_json, str):
            summary_json = json.loads(summary_json)

        st.markdown("### 📌 AI Summary")
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
