import streamlit as st
from snowflake.snowpark.context import get_active_session
import json

# -------------------------------------------------
# Page Configuration
# -------------------------------------------------
st.set_page_config(
    page_title="Policy Version Comparison",
    layout="wide"
)

session = get_active_session()

st.title("📄 Policy Version Comparison")

# =================================================
# SIDEBAR FILTERS
# =================================================

st.sidebar.header("🧩 Comparison Filters")

# LOB
lob_df = session.sql("""
    SELECT DISTINCT LOB
    FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
    ORDER BY LOB
""").to_pandas()

lob_list = lob_df["LOB"].dropna().tolist()
selected_lob = st.sidebar.selectbox("LOB", lob_list)

# STATE
state_df = session.sql(f"""
    SELECT DISTINCT STATE
    FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
    WHERE LOB = '{selected_lob}'
    ORDER BY STATE
""").to_pandas()

state_list = state_df["STATE"].dropna().tolist()
selected_state = st.sidebar.selectbox("State", state_list)

# POLICY
policy_df = session.sql(f"""
    SELECT DISTINCT POLICY_NAME
    FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
    WHERE LOB = '{selected_lob}'
    AND STATE = '{selected_state}'
    ORDER BY POLICY_NAME
""").to_pandas()

policy_list = policy_df["POLICY_NAME"].dropna().tolist()
selected_policy = st.sidebar.selectbox("Select Policy", policy_list)

# VERSIONS
version_df = session.sql(f"""
    SELECT VERSION, DOC_ID
    FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
    WHERE POLICY_NAME = '{selected_policy}'
    AND LOB = '{selected_lob}'
    AND STATE = '{selected_state}'
    ORDER BY VERSION
""").to_pandas()

if version_df.empty:
    st.warning("No versions found.")
    st.stop()

versions = version_df["VERSION"].tolist()

old_version = st.sidebar.selectbox("Old Version", versions)
latest_version = versions[-1]

old_doc_id = version_df[version_df["VERSION"] == old_version]["DOC_ID"].values[0]
new_doc_id = version_df[version_df["VERSION"] == latest_version]["DOC_ID"].values[0]

# =================================================
# ANALYZE BUTTON
# =================================================

if st.sidebar.button("Analyze Policy Impact"):

    if old_doc_id == new_doc_id:
        st.warning("Please select a different old version.")
        st.stop()

    st.markdown(f"### 📌 {selected_policy}")
    st.markdown(f"**Old Version:** {old_version} (DOC_ID: {old_doc_id})")
    st.markdown(f"**Latest Version:** {latest_version} (DOC_ID: {new_doc_id})")

    # -------------------------------------------------
    # Run Comparison
    # -------------------------------------------------
    session.sql(f"""
        CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.COMPARE_POLICY_VERSIONS(
            {old_doc_id},
            {new_doc_id}
        )
    """).collect()

    # -------------------------------------------------
    # Fetch Differences
    # -------------------------------------------------
    diff_df = session.sql(f"""
        SELECT OLD_CLAUSE,
               NEW_CLAUSE
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_VERSION_DIFFS
        WHERE OLD_DOC_ID = {old_doc_id}
        AND NEW_DOC_ID = {new_doc_id}
    """).to_pandas()

    st.markdown("## 📊 Version Comparison")

    if diff_df.empty:
        st.info("No differences found.")
    else:

        # PURE HTML TABLE — NO HEIGHT LIMIT — NO SCROLL
        html = """
        <style>
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
            vertical-align: top;
            word-wrap: break-word;
            white-space: normal;
        }
        th {
            background-color: #f2f2f2;
        }
        </style>

        <table>
        <thead>
            <tr>
                <th>Old Version</th>
                <th>New Version</th>
            </tr>
        </thead>
        <tbody>
        """

        for _, row in diff_df.iterrows():
            old_clause = row["OLD_CLAUSE"] if row["OLD_CLAUSE"] else ""
            new_clause = row["NEW_CLAUSE"] if row["NEW_CLAUSE"] else ""

            html += f"""
            <tr>
                <td>{old_clause}</td>
                <td>{new_clause}</td>
            </tr>
            """

        html += "</tbody></table>"

        st.markdown(html, unsafe_allow_html=True)

    # -------------------------------------------------
    # Generate Summary
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

    st.markdown("## 📌 Summary")
    st.info(summary_json.get("summary", "No summary generated."))

    st.markdown("## ⚠ Risk Highlights")

    risks = summary_json.get("risk_highlights", [])

    if risks:
        for r in risks:
            st.markdown(f"- {r}")
    else:
        st.write("No risks identified.")

st.divider()
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
