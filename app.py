import streamlit as st
from snowflake.snowpark.context import get_active_session
import json

# -------------------------------------------------
# Page Config (compact)
# -------------------------------------------------
st.set_page_config(
    page_title="Policy Control",
    layout="wide"
)

# -------------------------------------------------
# Session Init
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
if "username" not in st.session_state:
    st.session_state["username"] = None
if "app_role" not in st.session_state:
    st.session_state["app_role"] = None

session = get_active_session()

# -------------------------------------------------
# Role Check
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
# Filters Loader
# -------------------------------------------------
def load_filters():
    df = session.sql("""
        SELECT DISTINCT LOB, STATE
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
        ORDER BY 1,2
    """).to_pandas()

    return {
        "LOB": sorted(df["LOB"].dropna().unique()),
        "STATE": sorted(df["STATE"].dropna().unique())
    }

# -------------------------------------------------
# LOGIN
# -------------------------------------------------
if not st.session_state["authenticated"]:

    st.title("Login")

    with st.form("login_form"):
        user = st.text_input("Username")
        login_btn = st.form_submit_button("Login")

    if login_btn:
        role = get_app_role(user)

        if not role:
            st.error("Not Authorized")
            st.stop()

        st.session_state["authenticated"] = True
        st.session_state["username"] = user
        st.session_state["app_role"] = role

    st.stop()

# -------------------------------------------------
# Sidebar
# -------------------------------------------------
st.sidebar.write("User:", st.session_state["username"])
if st.sidebar.button("Logout"):
    st.session_state.clear()
    st.experimental_rerun()

st.sidebar.header("Menu")

app_mode = st.sidebar.radio(
    "Select Option",
    ["Search Policy", "Analyze Policy Changes"]
)

filters = load_filters()

# =================================================
# SEARCH MODE (unchanged simple version)
# =================================================
if app_mode == "Search Policy":

    search_text = st.sidebar.text_input("Search")
    lob = st.sidebar.selectbox("LOB", filters["LOB"])
    state = st.sidebar.selectbox("State", filters["STATE"])

    version_df = session.sql(f"""
        SELECT DISTINCT VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
        WHERE LOB='{lob}' AND STATE='{state}'
        ORDER BY VERSION
    """).to_pandas()

    versions = version_df["VERSION"].tolist()
    version = st.sidebar.selectbox("Version", versions)

    if st.sidebar.button("Search"):

        result = session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC.SEARCH_POLICY_CLAUSE(
                '{search_text}',
                '{state}',
                '{lob}',
                '{version}'
            )
        """).to_pandas()

        st.dataframe(result)

# =================================================
# ANALYZE POLICY CHANGES (FINAL CLEAN VERSION)
# =================================================
if app_mode == "Analyze Policy Changes":

    st.sidebar.header("Comparison Filters")

    lob = st.sidebar.selectbox("LOB", filters["LOB"])
    state = st.sidebar.selectbox("State", filters["STATE"])

    file_df = session.sql(f"""
        SELECT DISTINCT FILE_NAME
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
        WHERE LOB='{lob}' AND STATE='{state}'
        ORDER BY FILE_NAME
    """).to_pandas()

    filenames = file_df["FILE_NAME"].tolist()
    file_name = st.sidebar.selectbox("Policy File", filenames)

    version_df = session.sql(f"""
        SELECT DISTINCT VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
        WHERE FILE_NAME='{file_name}'
        ORDER BY VERSION
    """).to_pandas()

    versions = version_df["VERSION"].tolist()

    old_version = st.sidebar.selectbox("Old Version", versions)
    latest_version = sorted(versions)[-1] if versions else None
    st.sidebar.write("Latest:", latest_version)

    def get_doc_id(file_name, version):
        df = session.sql(f"""
            SELECT DOC_ID
            FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
            WHERE FILE_NAME='{file_name}'
            AND VERSION='{version}'
        """).to_pandas()
        return df.iloc[0]["DOC_ID"] if not df.empty else None

    old_doc_id = get_doc_id(file_name, old_version)
    new_doc_id = get_doc_id(file_name, latest_version)

    analyze = st.sidebar.button("Analyze Policy Impact")

    if analyze:

        if not old_doc_id or not new_doc_id:
            st.error("Invalid selection")
            st.stop()

        # Run Diff
        session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.COMPARE_POLICY_VERSIONS(
                {old_doc_id}, {new_doc_id}
            )
        """).collect()

        # Run Summary
        summary_res = session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.GENERATE_CHANGE_SUMMARY(
                {old_doc_id}, {new_doc_id}
            )
        """).collect()

        summary_json = summary_res[0][0]
        if isinstance(summary_json, str):
            summary_json = json.loads(summary_json)

        # CENTER NARROW LAYOUT
        left, center, right = st.columns([1, 2, 1])

        with center:

            # ===============================
            # CHANGE SUMMARY (CLAUSE DIFF)
            # ===============================
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
                        f"<div style='background:#ffe6e6;padding:6px;border-radius:4px;font-size:13px'>{old_clause}</div>",
                        unsafe_allow_html=True
                    )

                elif change == "added":
                    col2.markdown(
                        f"<div style='background:#e6ffe6;padding:6px;border-radius:4px;font-size:13px'>{new_clause}</div>",
                        unsafe_allow_html=True
                    )

                elif change == "modified":
                    col1.markdown(
                        f"<div style='background:#fff8dc;padding:6px;border-radius:4px;font-size:13px'>{old_clause}</div>",
                        unsafe_allow_html=True
                    )
                    col2.markdown(
                        f"<div style='background:#fff8dc;padding:6px;border-radius:4px;font-size:13px'>{new_clause}</div>",
                        unsafe_allow_html=True
                    )

            st.markdown("---")

            # ===============================
            # COMPARISON (LLM SUMMARY)
            # ===============================
            st.markdown("### Comparison")

            st.markdown(
                f"<div style='background:#eef3f9;padding:10px;border-radius:6px;font-size:14px'>{summary_json.get('summary','')}</div>",
                unsafe_allow_html=True
            )

            risks = summary_json.get("risk_highlights", [])
            if risks:
                st.markdown("**Risk Highlights**")
                for r in risks:
                    st.markdown(f"- {r}")

st.caption("Powered by Snowflake Cortex")
