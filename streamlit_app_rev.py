import streamlit as st

st.set_page_config(layout="wide", initial_sidebar_state="expanded")

# Display main content
st.title("Welcome to uncertainty!!!")
st.write("Use The sidebar to Move to Other Pages")
st.write("Move to 'Running Script' Pages to Run the Calculation")
st.write("Move to 'Summary' Pages to See the Results")

# --- PAGE SETUP ---
about_page = st.Page(
    "views/Running_Script_rev.py",
    title="Running Script",
    icon=":material/account_circle:",
)
project_1_page = st.Page(
    "views/Summary_rev.py",
    title="Summary",
    icon=":material/bar_chart:",
)
project_2_page = st.Page(
    "views/Summary_per_cohort.py",
    title="Summary_per_cohort",
    icon=":material/bar_chart:",
)

# --- NAVIGATION SETUP [WITHOUT SECTIONS] ---
# pg = st.navigation(pages=[about_page, project_1_page, project_2_page])

# --- NAVIGATION SETUP [WITH SECTIONS]---
pg = st.navigation(
    {
        "Menu": [about_page, project_1_page, project_2_page],
    }
)

# --- SHARED ON ALL PAGES ---
# st.logo("assets/codingisfun_logo.png")
st.sidebar.markdown("Reference from [Sven](https://youtube.com/@codingisfun)")


# --- RUN NAVIGATION ---
pg.run()
