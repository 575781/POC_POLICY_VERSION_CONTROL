import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import html   # ✅ NEW (for escaping)

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
# ✅ FIXED DIFF FUNCTION
# -------------------------------------------------
def generate_diff_html(df):

    html_content = """
    <style>
    .diff-table {
        width: 100%;
        border-collapse: collapse;
        font-family: Arial, sans-serif;
    }
    .diff-table th {
        background-color: #1f2937;
        color: white;
        padding: 10px;
        text-align: left;
    }
    .diff-table td {
        padding: 10px;
        border-bottom: 1px solid #ddd;
        vertical-align: top;
        white-space: pre-wrap;
    }
    .added {
        background-color: #d1fae5;
    }
    .removed {
        background-color: #fee2e2;
    }
    .modified {
        background-color: #fef9c3;
    }
    </style>

    <table class="diff-table">
    <tr>
        <th>Previous Version</th>
        <th>Latest Version</th>
        <th>Change Type</th>
    </tr>
    """

    for _, row in df.iterrows():

        old_raw = row["Previous Version"]
        new_raw = row["Latest Version"]

        # ✅ CRITICAL FIX: Escape HTML
        old = html.escape(str(old_raw)) if old_raw else ""
        new = html.escape(str(new_raw)) if new_raw else ""

        if old and not new:
            row_class = "removed"
            change = "Removed"
        elif new and not old:
            row_class = "added"
            change = "Added"
        else:
            row_class = "modified"
            change = "Modified"

        html_content += f"""
        <tr class="{row_class}">
            <td>{old}</td>
            <td>{new}</td>
            <td><b>{change}</b></td>
        </tr>
        """

    html_content += "</table>"

    return html_content

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
# SEARCH POLICY MODE
# =================================================
if app_mode == "Search Policy":

    st.title("📄 Policy & Control Search")

    with st.sidebar:
        st.header("🔎 Search Filters")

        try:
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

            selected_policy = st.selectbox("Policy", policy_df["POLICY_NAME"].dropna().tolist())

            version_df = session.sql("""
                SELECT DISTINCT VERSION
                FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
                WHERE POLICY_NAME = :1
                ORDER BY VERSION
            """, [selected_policy]).to_pandas()

            selected_version = st.selectbox("Version", version_df["VERSION"].tolist())

            search_text = st.text_input("Search Clause")

            if st.button("🔍 Search"):

                result_df = session.sql("""
                    CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.SEARCH_POLICY_CLAUSE(
                        :1, :2, :3, :4
                    )
                """, [search_text, selected_state, selected_lob, selected_version]).to_pandas()

                if result_df.empty:
                    st.warning("No matching clauses found.")
                else:
                    st.success(f"{len(result_df)} results found")
                    st.dataframe(result_df, use_container_width=True)

        except Exception as e:
            st.error(f"Error: {str(e)}")

# =================================================
# ANALYZE POLICY CHANGES MODE
# =================================================
if app_mode == "Analyze Policy Changes":

    st.title("📄 Policy Version Comparison")

    with st.sidebar:
        st.header("🧩 Comparison Filters")

        try:
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

        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.stop()

    try:
        version_df = session.sql("""
            SELECT VERSION, DOC_ID
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
            WHERE POLICY_NAME = :1 AND LOB = :2 AND STATE = :3
            ORDER BY VERSION
        """, [selected_policy, selected_lob, selected_state]).to_pandas()

        if len(version_df) < 2:
            st.warning("At least two versions required for comparison.")
            st.stop()

        version_df = version_df.sort_values("VERSION")

        latest_row = version_df.iloc[-1]
        previous_row = version_df.iloc[-2]

        latest_version = latest_row["VERSION"]
        previous_version = previous_row["VERSION"]

        # ✅ FIX: numpy → python int
        old_doc_id = int(previous_row["DOC_ID"]) if previous_row["DOC_ID"] is not None else None
        new_doc_id = int(latest_row["DOC_ID"]) if latest_row["DOC_ID"] is not None else None

        st.sidebar.markdown("### 🔎 Auto Comparison")
        st.sidebar.write(f"Previous Version: {previous_version}")
        st.sidebar.write(f"Latest Version: {latest_version}")

        if st.sidebar.button("Analyze Policy Impact"):

            st.markdown(f"### 📌 Policy: {selected_policy}")
            st.markdown(f"**Previous Version:** {previous_version} (DOC_ID: {old_doc_id})")
            st.markdown(f"**Latest Version:** {latest_version} (DOC_ID: {new_doc_id})")

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
                st.info("No differences found between selected versions.")
            else:
                st.success(f"{len(diff_df)} changes identified")

                # ✅ FINAL DIFF VIEW
                styled_html = generate_diff_html(diff_df)
                st.markdown(styled_html, unsafe_allow_html=True)

            summary_result = session.sql("""
                CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.GENERATE_CHANGE_SUMMARY(:1, :2)
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
        st.error(f"Error: {str(e)}")

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
