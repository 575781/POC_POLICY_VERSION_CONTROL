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

# -------------------------------------------------
# Basic Login (Optional – Keep if required)
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = True  # Simplified

st.title("📄 Policy Version Comparison")

# =================================================
# FILTER SECTION
# =================================================

st.sidebar.header("🧩 Comparison Filters")

# 1️⃣ LOB
lob_df = session.sql("""
    SELECT DISTINCT LOB
    FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
    ORDER BY LOB
""").to_pandas()

lob_list = lob_df["LOB"].dropna().tolist()
selected_lob = st.sidebar.selectbox("LOB", lob_list)

# 2️⃣ STATE
state_df = session.sql(f"""
    SELECT DISTINCT STATE
    FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
    WHERE LOB = '{selected_lob}'
    ORDER BY STATE
""").to_pandas()

state_list = state_df["STATE"].dropna().tolist()
selected_state = st.sidebar.selectbox("State", state_list)

# 3️⃣ POLICY NAME
policy_df = session.sql(f"""
    SELECT DISTINCT POLICY_NAME
    FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
    WHERE LOB = '{selected_lob}'
    AND STATE = '{selected_state}'
    ORDER BY POLICY_NAME
""").to_pandas()

policy_list = policy_df["POLICY_NAME"].dropna().tolist()
selected_policy = st.sidebar.selectbox("Select Policy", policy_list)

# 4️⃣ FETCH VERSIONS
version_df = session.sql(f"""
    SELECT VERSION, DOC_ID
    FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
    WHERE POLICY_NAME = '{selected_policy}'
    AND LOB = '{selected_lob}'
    AND STATE = '{selected_state}'
    ORDER BY VERSION
""").to_pandas()

if version_df.empty:
    st.warning("No versions found for selected policy.")
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

    st.markdown(f"### 📌 Policy: {selected_policy}")
    st.markdown(f"**Old Version:** {old_version} (DOC_ID: {old_doc_id})")
    st.markdown(f"**Latest Version:** {latest_version} (DOC_ID: {new_doc_id})")

    # -------------------------------------------------
    # Run Comparison Procedure
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
        SELECT CHANGE_TYPE,
               OLD_CLAUSE,
               NEW_CLAUSE
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_VERSION_DIFFS
        WHERE OLD_DOC_ID = {old_doc_id}
        AND NEW_DOC_ID = {new_doc_id}
    """).to_pandas()

    st.markdown("## 📊 Version Comparison")

    if diff_df.empty:
        st.info("No differences found between selected versions.")
    else:
        for _, row in diff_df.iterrows():

            change_type = row["CHANGE_TYPE"].lower()
            old_clause = row["OLD_CLAUSE"]
            new_clause = row["NEW_CLAUSE"]

            col1, col2 = st.columns(2)

            # 🟥 REMOVED
            if change_type == "removed":
                with col1:
                    st.markdown(
                        f"""
                        <div style="
                            background-color:#ffcccc;
                            padding:10px;
                            border-radius:6px;
                            margin-bottom:6px;">
                        {old_clause}
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                with col2:
                    st.write("")

            # 🟩 ADDED
            elif change_type == "added":
                with col1:
                    st.write("")
                with col2:
                    st.markdown(
                        f"""
                        <div style="
                            background-color:#ccffcc;
                            padding:10px;
                            border-radius:6px;
                            margin-bottom:6px;">
                        {new_clause}
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

            # MODIFIED / SAME
            else:
                with col1:
                    st.markdown(
                        f"""
                        <div style="
                            padding:10px;
                            border-radius:6px;
                            margin-bottom:6px;
                            border:1px solid #ddd;">
                        {old_clause}
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                with col2:
                    st.markdown(
                        f"""
                        <div style="
                            padding:10px;
                            border-radius:6px;
                            margin-bottom:6px;
                            border:1px solid #ddd;">
                        {new_clause}
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

    # -------------------------------------------------
    # Generate AI Summary
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

    st.markdown("## 📌 AI Summary")
    st.info(summary_json.get("summary", "No summary generated."))

    st.markdown("## ⚠ Risk Highlights")
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
