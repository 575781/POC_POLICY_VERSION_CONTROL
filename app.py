import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import pandas as pd
import logging

# -------------------------------------------------
# CONFIG + LOGGING
# -------------------------------------------------
DB = "AI_POC_DB"
SCHEMA = "HEALTH_POLICY_POC_CHANGE_SUMMARY"

logging.basicConfig(level=logging.INFO)

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
# CACHE HELPERS (Performance Boost)
# -------------------------------------------------
@st.cache_data
def get_lob_data():
    return session.sql(f"""
        SELECT DISTINCT LOB
        FROM {DB}.{SCHEMA}.DOCUMENT_METADATA
        ORDER BY LOB
    """).to_pandas()

@st.cache_data
def get_state_data(lob):
    return session.sql(f"""
        SELECT DISTINCT STATE
        FROM {DB}.{SCHEMA}.DOCUMENT_METADATA
        WHERE LOB = :1
        ORDER BY STATE
    """, [lob]).to_pandas()

@st.cache_data
def get_policy_data(lob, state):
    return session.sql(f"""
        SELECT DISTINCT POLICY_NAME
        FROM {DB}.{SCHEMA}.DOCUMENT_METADATA
        WHERE LOB = :1 AND STATE = :2
        ORDER BY POLICY_NAME
    """, [lob, state]).to_pandas()

@st.cache_data
def get_version_data(policy, lob, state):
    return session.sql(f"""
        SELECT VERSION, DOC_ID
        FROM {DB}.{SCHEMA}.DOCUMENT_METADATA
        WHERE POLICY_NAME = :1 AND LOB = :2 AND STATE = :3
        ORDER BY VERSION
    """, [policy, lob, state]).to_pandas()

# -------------------------------------------------
# DIFF STYLING FUNCTION (UNCHANGED)
# -------------------------------------------------
def style_diff(df):

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

    def highlight_row(row):
        if row["Change Type"] == "Added":
            return ["background-color: #d1fae5"] * len(row)
        elif row["Change Type"] == "Removed":
            return ["background-color: #fee2e2"] * len(row)
        else:
            return ["background-color: #fef9c3"] * len(row)

    return df.style.apply(highlight_row, axis=1)

# -------------------------------------------------
# Fetch App Role (UNCHANGED)
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
# LOGIN (UNCHANGED)
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
# SEARCH POLICY MODE
# =================================================
if app_mode == "Search Policy":

    st.title("📄 Policy & Control Search")

    with st.sidebar:
        st.header("🔎 Search Filters")

        try:
            lob_df = get_lob_data()
            selected_lob = st.selectbox("LOB", lob_df["LOB"].dropna().tolist())

            state_df = get_state_data(selected_lob)
            selected_state = st.selectbox("State", state_df["STATE"].dropna().tolist())

            policy_df = get_policy_data(selected_lob, selected_state)
            selected_policy = st.selectbox("Policy", policy_df["POLICY_NAME"].dropna().tolist())

            version_df = session.sql(f"""
                SELECT DISTINCT VERSION
                FROM {DB}.{SCHEMA}.DOCUMENT_METADATA
                WHERE POLICY_NAME = :1
                ORDER BY VERSION
            """, [selected_policy]).to_pandas()

            selected_version = st.selectbox("Version", version_df["VERSION"].tolist())

            search_text = st.text_input("Search Clause")

            if st.button("🔍 Search"):

                with st.spinner("Searching..."):

                    result_df = session.sql(f"""
                        CALL {DB}.{SCHEMA}.SEARCH_POLICY_CLAUSE(:1, :2, :3, :4)
                    """, [search_text, selected_state, selected_lob, selected_version]).to_pandas()

                    if result_df.empty:
                        st.warning("No matching clauses found.")
                    else:
                        st.success(f"{len(result_df)} results found")
                        st.dataframe(result_df, use_container_width=True)

        except Exception as e:
            logging.error(e)
            st.error("Search failed")

# =================================================
# ANALYZE POLICY CHANGES MODE
# =================================================
if app_mode == "Analyze Policy Changes":

    st.title("📄 Policy Version Comparison")

    with st.sidebar:
        st.header("🧩 Comparison Filters")

        try:
            lob_df = get_lob_data()
            selected_lob = st.selectbox("LOB", lob_df["LOB"].dropna().tolist())

            state_df = get_state_data(selected_lob)
            selected_state = st.selectbox("State", state_df["STATE"].dropna().tolist())

            policy_df = get_policy_data(selected_lob, selected_state)
            selected_policy = st.selectbox("Select Policy", policy_df["POLICY_NAME"].dropna().tolist())

        except Exception as e:
            logging.error(e)
            st.error("Filter loading failed")
            st.stop()

    try:
        version_df = get_version_data(selected_policy, selected_lob, selected_state)

        if len(version_df) < 2:
            st.warning("At least two versions required for comparison.")
            st.stop()

        version_df = version_df.sort_values("VERSION")

        latest_row = version_df.iloc[-1]
        previous_row = version_df.iloc[-2]

        latest_version = latest_row["VERSION"]
        previous_version = previous_row["VERSION"]

        old_doc_id = int(previous_row["DOC_ID"])
        new_doc_id = int(latest_row["DOC_ID"])

        st.sidebar.markdown("### 🔎 Auto Comparison")
        st.sidebar.write(f"Latest Version: {latest_version}")
        st.sidebar.write(f"Previous Version: {previous_version}")

        if st.sidebar.button("Analyze Policy Impact"):

            logging.info(f"Comparing {old_doc_id} vs {new_doc_id}")

            st.markdown(f"### 📌 Policy: {selected_policy}")
            st.markdown(f"**Latest Version:** {latest_version} (DOC_ID: {new_doc_id})")
            st.markdown(f"**Previous Version:** {previous_version} (DOC_ID: {old_doc_id})")

            # Avoid duplicate execution
            count = session.sql(f"""
                SELECT COUNT(*) FROM {DB}.{SCHEMA}.POLICY_VERSION_DIFFS
                WHERE OLD_DOC_ID = :1 AND NEW_DOC_ID = :2
            """, [old_doc_id, new_doc_id]).collect()[0][0]

            if count == 0:
                session.sql(f"""
                    CALL {DB}.{SCHEMA}.COMPARE_POLICY_VERSIONS(:1, :2)
                """, [old_doc_id, new_doc_id]).collect()

            diff_df = session.sql(f"""
                SELECT OLD_CLAUSE AS "Previous Version",
                       NEW_CLAUSE AS "Latest Version"
                FROM {DB}.{SCHEMA}.POLICY_VERSION_DIFFS
                WHERE OLD_DOC_ID = :1 AND NEW_DOC_ID = :2
            """, [old_doc_id, new_doc_id]).to_pandas()

            st.markdown("### 📊 Version Comparison")

            if diff_df.empty:
                st.info("No differences found between selected versions.")
            else:
                html_table = style_diff(diff_df).to_html()
                st.markdown(html_table, unsafe_allow_html=True)

            summary_result = session.sql(f"""
                CALL {DB}.{SCHEMA}.GENERATE_CHANGE_SUMMARY(:1, :2)
            """, [old_doc_id, new_doc_id]).collect()

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

    except Exception as e:
        logging.error(e)
        st.error("Analysis failed")

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.divider()
st.caption("Production-ready • Snowflake Cortex • Streamlit")
