import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import logging

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
DB = "AI_POC_DB"
SCHEMA = "HEALTH_POLICY_POC_CHANGE_SUMMARY"

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)

# -------------------------------------------------
# Page Configuration
# -------------------------------------------------
st.set_page_config(
    page_title="Policy Intelligence Platform",
    layout="wide"
)

# -------------------------------------------------
# SESSION INIT
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
if "username" not in st.session_state:
    st.session_state["username"] = None

session = get_active_session()

# -------------------------------------------------
# SAFE QUERY HELPERS (CACHED)
# -------------------------------------------------
@st.cache_data
def get_lobs():
    return session.sql(f"""
        SELECT DISTINCT LOB 
        FROM {DB}.{SCHEMA}.DOCUMENT_METADATA
        ORDER BY LOB
    """).to_pandas()

@st.cache_data
def get_states(lob):
    return session.sql(f"""
        SELECT DISTINCT STATE 
        FROM {DB}.{SCHEMA}.DOCUMENT_METADATA
        WHERE LOB = :1
        ORDER BY STATE
    """, [lob]).to_pandas()

@st.cache_data
def get_policies(lob, state):
    return session.sql(f"""
        SELECT DISTINCT POLICY_NAME 
        FROM {DB}.{SCHEMA}.DOCUMENT_METADATA
        WHERE LOB = :1 AND STATE = :2
        ORDER BY POLICY_NAME
    """, [lob, state]).to_pandas()

@st.cache_data
def get_versions(lob, state, policy):
    return session.sql(f"""
        SELECT VERSION, DOC_ID
        FROM {DB}.{SCHEMA}.DOCUMENT_METADATA
        WHERE LOB = :1 AND STATE = :2 AND POLICY_NAME = :3
        ORDER BY VERSION
    """, [lob, state, policy]).to_pandas()

# -------------------------------------------------
# LOGIN
# -------------------------------------------------
def login():
    st.title("🔐 Login")

    with st.form("login"):
        user = st.text_input("Username")
        submit = st.form_submit_button("Login")

    if submit:
        if not user:
            st.warning("Enter username")
            return False

        st.session_state["authenticated"] = True
        st.session_state["username"] = user
        return True

    return False

if not st.session_state["authenticated"]:
    if not login():
        st.stop()

# -------------------------------------------------
# SIDEBAR
# -------------------------------------------------
st.sidebar.success(f"👤 {st.session_state['username']}")

if st.sidebar.button("Logout"):
    st.session_state.clear()
    st.experimental_rerun()

app_mode = st.sidebar.radio(
    "Menu",
    ["Search Policy", "Analyze Policy Changes"]
)

# =================================================
# SEARCH MODE
# =================================================
if app_mode == "Search Policy":

    st.title("📄 Policy Search")

    try:
        lob = st.sidebar.selectbox("LOB", get_lobs()["LOB"].dropna())

        state = st.sidebar.selectbox(
            "State", get_states(lob)["STATE"].dropna()
        )

        policy = st.sidebar.selectbox(
            "Policy", get_policies(lob, state)["POLICY_NAME"].dropna()
        )

        version_df = get_versions(lob, state, policy)
        version = st.sidebar.selectbox("Version", version_df["VERSION"])

        search_text = st.sidebar.text_input("Search")

        if st.sidebar.button("Search"):

            with st.spinner("Searching..."):

                result = session.sql(f"""
                    CALL {DB}.{SCHEMA}.SEARCH_POLICY_CLAUSE(
                        :1, :2, :3, :4
                    )
                """, [search_text, state, lob, version]).to_pandas()

                if result.empty:
                    st.warning("No results found")
                else:
                    st.dataframe(result, use_container_width=True)

    except Exception as e:
        logging.error(e)
        st.error("Search failed")

# =================================================
# ANALYZE MODE
# =================================================
if app_mode == "Analyze Policy Changes":

    st.title("📊 Policy Version Comparison")

    try:
        lob = st.sidebar.selectbox("LOB", get_lobs()["LOB"].dropna())

        state = st.sidebar.selectbox(
            "State", get_states(lob)["STATE"].dropna()
        )

        policy = st.sidebar.selectbox(
            "Policy", get_policies(lob, state)["POLICY_NAME"].dropna()
        )

        version_df = get_versions(lob, state, policy)

        if len(version_df) < 2:
            st.warning("Need at least 2 versions")
            st.stop()

        version_df = version_df.sort_values("VERSION")

        latest = version_df.iloc[-1]
        previous = version_df.iloc[-2]

        new_doc_id = int(latest["DOC_ID"])
        old_doc_id = int(previous["DOC_ID"])

        st.sidebar.info(f"Latest: {latest['VERSION']}")
        st.sidebar.info(f"Previous: {previous['VERSION']}")

        if st.sidebar.button("Analyze"):

            logging.info(f"Comparing {old_doc_id} vs {new_doc_id}")

            with st.spinner("Analyzing policy changes..."):

                # Avoid re-run if already exists
                count = session.sql(f"""
                    SELECT COUNT(*) 
                    FROM {DB}.{SCHEMA}.POLICY_VERSION_DIFFS
                    WHERE OLD_DOC_ID = :1 AND NEW_DOC_ID = :2
                """, [old_doc_id, new_doc_id]).collect()[0][0]

                if count == 0:
                    session.sql(f"""
                        CALL {DB}.{SCHEMA}.COMPARE_POLICY_VERSIONS(:1, :2)
                    """, [old_doc_id, new_doc_id]).collect()

                diff_df = session.sql(f"""
                    SELECT OLD_CLAUSE, NEW_CLAUSE
                    FROM {DB}.{SCHEMA}.POLICY_VERSION_DIFFS
                    WHERE OLD_DOC_ID = :1 AND NEW_DOC_ID = :2
                """, [old_doc_id, new_doc_id]).to_pandas()

                st.subheader("📊 Version Comparison")

                if diff_df.empty:
                    st.info("No differences found")
                else:
                    st.dataframe(diff_df, use_container_width=True)

                # Summary
                summary = session.sql(f"""
                    CALL {DB}.{SCHEMA}.GENERATE_CHANGE_SUMMARY(:1, :2)
                """, [old_doc_id, new_doc_id]).collect()[0][0]

                if isinstance(summary, str):
                    summary = json.loads(summary)

                st.subheader("📌 Summary")
                st.info(summary.get("summary", "No summary"))

                st.subheader("⚠ Risk Highlights")

                risks = summary.get("risk_highlights", [])
                if risks:
                    for r in risks:
                        st.markdown(f"- {r}")
                else:
                    st.write("No risks identified")

    except Exception as e:
        logging.error(e)
        st.error("Analysis failed")

# -------------------------------------------------
# FOOTER
# -------------------------------------------------
st.divider()
st.caption("Production-ready • Snowflake Cortex AI")
