import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import html

# -------------------------------------------------
# Page Config
# -------------------------------------------------
st.set_page_config(page_title="Policy Search", layout="wide")

# -------------------------------------------------
# Session Init
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "username" not in st.session_state:
    st.session_state.username = None

session = get_active_session()

# -------------------------------------------------
# SAFE DIFF HTML (FINAL FIX)
# -------------------------------------------------
def generate_diff_html(df):

    def get_change_type(old, new):
        if old and not new:
            return "Removed"
        elif new and not old:
            return "Added"
        else:
            return "Modified"

    html_block = """
    <style>
    table {
        width: 100%;
        border-collapse: collapse;
        font-family: Arial;
        table-layout: fixed;
    }
    th {
        background: #1f2937;
        color: white;
        padding: 10px;
        text-align: left;
    }
    td {
        padding: 10px;
        border-bottom: 1px solid #ddd;
        vertical-align: top;
    }
    .cell {
        white-space: pre-wrap;
        word-break: break-word;
    }
    </style>

    <table>
    <tr>
        <th>Previous Version</th>
        <th>Latest Version</th>
        <th>Change</th>
    </tr>
    """

    for _, row in df.iterrows():
        old_raw = row["Previous Version"]
        new_raw = row["Latest Version"]

        old = html.escape(str(old_raw)) if old_raw else ""
        new = html.escape(str(new_raw)) if new_raw else ""

        change = get_change_type(old, new)

        color = {
            "Added": "#d1fae5",
            "Removed": "#fee2e2",
            "Modified": "#fef9c3"
        }[change]

        html_block += f"""
        <tr style="background:{color}">
            <td><div class="cell">{old}</div></td>
            <td><div class="cell">{new}</div></td>
            <td><b>{change}</b></td>
        </tr>
        """

    html_block += "</table>"
    return html_block


# -------------------------------------------------
# LOGIN
# -------------------------------------------------
if not st.session_state.authenticated:

    st.title("🔐 Login")

    user = st.text_input("Username")
    if st.button("Login"):
        if user:
            st.session_state.authenticated = True
            st.session_state.username = user
            st.rerun()

    st.stop()

# -------------------------------------------------
# SIDEBAR
# -------------------------------------------------
st.sidebar.success(f"User: {st.session_state.username}")

if st.sidebar.button("Logout"):
    st.session_state.clear()
    st.rerun()

mode = st.sidebar.radio("Menu", ["Search Policy", "Analyze Policy Changes"])

# =================================================
# ANALYZE MODE (FOCUS AREA)
# =================================================
if mode == "Analyze Policy Changes":

    st.title("📊 Policy Version Comparison")

    # Filters
    lob = st.sidebar.selectbox("LOB", ["HEALTH"])
    state = st.sidebar.selectbox("State", ["KA"])
    policy = st.sidebar.selectbox("Policy", ["Apex Cardiac"])

    # Mock version data (replace with Snowflake query)
    old_doc_id = 1
    new_doc_id = 2

    if st.sidebar.button("Analyze Policy Impact"):

        # -------------------------------------------------
        # FETCH DIFF
        # -------------------------------------------------
        diff_df = session.sql("""
            SELECT OLD_CLAUSE AS "Previous Version",
                   NEW_CLAUSE AS "Latest Version"
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_VERSION_DIFFS
            WHERE OLD_DOC_ID = :1 AND NEW_DOC_ID = :2
        """, [old_doc_id, new_doc_id]).to_pandas()

        st.subheader("📌 Version Comparison")

        if diff_df.empty:
            st.info("No differences found.")
        else:
            html_table = generate_diff_html(diff_df)
            st.markdown(html_table, unsafe_allow_html=True)

        # -------------------------------------------------
        # SUMMARY (NOW ALWAYS VISIBLE)
        # -------------------------------------------------
        summary_result = session.sql("""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.GENERATE_CHANGE_SUMMARY(:1, :2)
        """, [old_doc_id, new_doc_id]).collect()

        summary_json = summary_result[0][0]
        if isinstance(summary_json, str):
            summary_json = json.loads(summary_json)

        st.subheader("📌 Summary")
        st.info(summary_json.get("summary", "No summary generated."))

        # -------------------------------------------------
        # RISKS (NOW ALWAYS VISIBLE)
        # -------------------------------------------------
        st.subheader("⚠ Risk Highlights")

        risks = summary_json.get("risk_highlights", [])
        if risks:
            for r in risks:
                st.markdown(f"- {r}")
        else:
            st.write("No risks identified.")

# -------------------------------------------------
# FOOTER
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex")
