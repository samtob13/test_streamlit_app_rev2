import pandas as pd
import numpy as np
import streamlit as st
import datetime
import math
import copy
import timeit
import itertools
# from sqlalchemy import create_engine
import subprocess
import sys
import plotly.express as px
# import gspread
# from oauth2client.service_account import ServiceAccountCredentials
# from gspread_dataframe import set_with_dataframe
# import toml
# import json

# secrets = toml.load(".streamlit/secrets.toml")
# credentials_json = secrets["google_service_account"]["credentials"]
# credentials_dict = json.loads(credentials_json)
# client = gspread.service_account_from_dict(credentials_dict)
# sheet = client.open("balance_sheet_total").sheet1


# scopes      = ['https://spreadsheets.google.com/feeds', 
#                    'https://www.googleapis.com/auth/spreadsheets', 
#                    'https://www.googleapis.com/auth/drive.file', 
#                    'https://www.googleapis.com/auth/drive']

# creds       = ServiceAccountCredentials.from_json_keyfile_name("./readstreamlit-41cda8d43512.json", scopes=scopes)

# def connect_to_googlesheet(spreadsheet_name, sheet_name):
#     file        = gspread.authorize(credentials=creds)
#     workbook    = file.open(spreadsheet_name)
#     return workbook.worksheet(sheet_name)

# GMM_VFA_df  = pd.DataFrame(connect_to_googlesheet('balance_sheet_total', 'GMM_VFA').get_all_records())
# PAA_df      = pd.DataFrame(connect_to_googlesheet('balance_sheet_total', 'PAA').get_all_records())
# set_with_dataframe(connect_to_googlesheet('balance_sheet_total', 'tes'), PAA_df)

# if st.button("Generate latest data"):
#     st.write("Running...")

#     # Run the external script and capture the output
#     result = subprocess.run([sys.executable, "summary_balance_sheet_rev.py"], capture_output=True, text=True)
#     st.write("Finished...")

# balance_sheet                   = pd.DataFrame(connect_to_googlesheet('balance_sheet_total', 'GMM_VFA').get_all_records())
# balance_sheet['IFRS_MONTH']     = pd.to_datetime(balance_sheet['IFRS_MONTH'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
balance_sheet                   = pd.read_csv("streamlit_output_rev/balance_sheet_total.csv")
balance_sheet['IFRS_MONTH']     = pd.to_datetime(balance_sheet['IFRS_MONTH'], errors='coerce', format='%Y-%m-%d')

# balance_sheet_PAA               = pd.DataFrame(connect_to_googlesheet('balance_sheet_total', 'PAA').get_all_records())
# balance_sheet_PAA['IFRS_MONTH'] = pd.to_datetime(balance_sheet_PAA['IFRS_MONTH'], errors='coerce', format='%Y-%m-%d')
balance_sheet_PAA               = pd.read_csv("streamlit_output_rev/balance_sheet_total_PAA.csv")
balance_sheet_PAA['IFRS_MONTH'] = pd.to_datetime(balance_sheet_PAA['IFRS_MONTH'], errors='coerce', format='%Y-%m-%d')

# csm                             = pd.DataFrame(connect_to_googlesheet('balance_sheet_total', 'CSM').get_all_records())
# csm['IFRS_MONTH']               = pd.to_datetime(csm['IFRS_MONTH'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
csm                             = pd.read_csv("streamlit_output_rev/csm_total.csv")
csm['IFRS_MONTH']               = pd.to_datetime(csm['IFRS_MONTH'], errors='coerce', format='%Y-%m-%d')

print(csm)

# tes = pd.DataFrame(connect_to_googlesheet('balance_sheet_total', 'tes').get_all_records())
# st.write(tes)

st.write("# Insurance Contract Reconciliation")
st.write("## Short Term Product")
# Filter by DATE using a select box
IFRS_MONTH_filter_PAA   = st.selectbox("SELECT DATE", options=["All"] + list(balance_sheet_PAA["IFRS_MONTH"].unique()))

if IFRS_MONTH_filter_PAA != "All":
    filtered_df_PAA = balance_sheet_PAA[balance_sheet_PAA["IFRS_MONTH"] == IFRS_MONTH_filter_PAA]
else:
    filtered_df_PAA = balance_sheet_PAA

# Filter by PRODUK using a select box
PRODUK_filter_PAA       = st.selectbox("SELECT PRODUK", options=["All"] + list(balance_sheet_PAA["NAMA_PRODUK"].unique()))

if PRODUK_filter_PAA != "All":
    filtered_df_PAA = filtered_df_PAA[filtered_df_PAA["NAMA_PRODUK"] == PRODUK_filter_PAA]
else:
    filtered_df_PAA = filtered_df_PAA

st.write("### Data Short Term Product")
st.write(filtered_df_PAA)

st.write("## Long Term Product")
# Filter by DATE using a select box
IFRS_MONTH_filter   = st.selectbox("SELECT DATE", options=["All"] + list(balance_sheet["IFRS_MONTH"].unique()))

if IFRS_MONTH_filter != "All":
    filtered_df = balance_sheet[balance_sheet["IFRS_MONTH"] == IFRS_MONTH_filter]
else:
    filtered_df = balance_sheet

# Filter by PRODUK using a select box
PRODUK_filter       = st.selectbox("SELECT PRODUK", options=["All"] + list(balance_sheet["NAMA_PRODUK"].unique()))

if PRODUK_filter != "All":
    filtered_df = filtered_df[filtered_df["NAMA_PRODUK"] == PRODUK_filter]
else:
    filtered_df = filtered_df

st.write("### Data Long Term Product")
st.write(filtered_df)

st.write("# CSM Roll Forward")
IFRS_MONTH_csm  = st.selectbox("SELECT DATE", options=list(csm["IFRS_MONTH"].unique()))
filtered_csm    = csm[csm["IFRS_MONTH"] == IFRS_MONTH_csm]
value_chart     = filtered_csm['Contractual Service Margin'].tolist()
percentages     = [x / sum(value_chart) * 100 for x in value_chart]
st.write(filtered_csm)

# Sample data
data = {
    'NAMA_PRODUK'   : filtered_csm['NAMA_PRODUK'].tolist(),
    'CSM'           : value_chart,
    'Percentage (%)': [f"{p:.2f}" for p in percentages]  # Format percentages
}

st.title("COMPARISON OF CSM FROM EACH PRODUCT")

# Create a plotly pie chart
fig = px.pie(data, values='CSM', names='NAMA_PRODUK', title="comparison of CSM in " + str(IFRS_MONTH_csm))
st.plotly_chart(fig)

# Display the table below the chart
st.subheader("Data Summary")
st.dataframe(data, use_container_width=True)


PRODUCT_csm_line    = st.selectbox("SELECT PRODUCT", options=list(csm["NAMA_PRODUK"].unique()))
filtered_csm_line   = csm[csm["NAMA_PRODUK"] == PRODUCT_csm_line]

st.title("CSM MOVEMENT OVER TIME")

# Create sample data
data = {
    'IFRS_MONTH': filtered_csm_line['IFRS_MONTH'].tolist(),
    'CSM'       : filtered_csm_line['Contractual Service Margin'].tolist()
}

# Create the line chart
fig = px.line(data, x='IFRS_MONTH', y='CSM', title="CSM MOVEMENT FOR " + PRODUCT_csm_line, markers=True)
st.plotly_chart(fig)
