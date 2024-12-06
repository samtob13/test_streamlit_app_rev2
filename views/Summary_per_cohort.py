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
import plotly.graph_objects as go
import locale

# import gspread
# from oauth2client.service_account import ServiceAccountCredentials
# from gspread_dataframe import set_with_dataframe
# import toml
# import json

def round_school(x):
    i, f = divmod(x, 1)
    return int(i + ((f >= 0.5) if (x > 0) else (f > 0.5)))

def round_school_sig(x,y):
    x    = int(x * 10 ** (y + 1)) / 10
    i, f = divmod(x, 1)
    return int(i + ((f >= 0.5) if (x > 0) else (f > 0.5))) / (10 ** y)


balance_sheet                   = pd.read_csv("streamlit_output_rev/balance_sheet_total_per_cohort.csv")
balance_sheet['IFRS_MONTH']     = pd.to_datetime(balance_sheet['IFRS_MONTH'], errors='coerce', format='%Y-%m-%d')

csm                             = pd.read_csv("streamlit_output_rev/csm_total_per_cohort.csv")
csm['IFRS_MONTH']               = pd.to_datetime(csm['IFRS_MONTH'], errors='coerce', format='%Y-%m-%d')
IFRS_MONTH_list                 = csm["IFRS_MONTH"].unique()

# tes = pd.DataFrame(connect_to_googlesheet('balance_sheet_total', 'tes').get_all_records())
# st.write(tes)

st.write("# Insurance Contract Reconciliation")
st.write("## Long Term Product")
# Filter by DATE using a select box
IFRS_MONTH_filter   = st.selectbox("SELECT DATE", options=["All"] + list(balance_sheet["IFRS_MONTH"].unique()))

# Filter by IFRS_MONTH using a select box
if IFRS_MONTH_filter != "All":
    filtered_df = balance_sheet[balance_sheet["IFRS_MONTH"] == IFRS_MONTH_filter]
else:
    filtered_df = balance_sheet

# Filter by PRODUK using a select box
PRODUK_filter       = st.selectbox("SELECT PRODUK", options=["All"] + list(filtered_df["NAMA_PRODUK"].unique()))
if PRODUK_filter != "All":
    filtered_df = filtered_df[filtered_df["NAMA_PRODUK"] == PRODUK_filter]
else:
    filtered_df = filtered_df

# Filter by KEY using a select box
KEY_filter          = st.selectbox("SELECT KEY", options=["All"] + list(filtered_df["KEY"].unique()))
if KEY_filter != "All":
    filtered_df = filtered_df[filtered_df["KEY"] == KEY_filter]
else:
    filtered_df = filtered_df

st.write("### Data Long Term Product")
st.write(filtered_df)

st.write("# CSM Roll Forward")
csm['Contractual Service Margin_Begin'] = csm['Contractual Service Margin'] - csm['Amortisasi_CSM_Release'] - csm['Efek_Perubahan_Varians_n_Asumsi_Ekonomi'] - csm['Accretion_Interest_unwind'] - csm['Kontrak_Baru_Periode_Berjalan']
csm                                     = csm[["IFRS_MONTH", "NAMA_PRODUK", "KEY", 
                                               "Contractual Service Margin_Begin", "Kontrak_Baru_Periode_Berjalan", 
                                               "Accretion_Interest_unwind", "Efek_Perubahan_Varians_n_Asumsi_Ekonomi", 
                                               "Amortisasi_CSM_Release", "Contractual Service Margin"]]

# IFRS_MONTH_csm                          = st.selectbox("SELECT DATE", options=list(csm["IFRS_MONTH"].unique()))
# PRODUK_csm                              = st.selectbox("SELECT PRODUK", options=list(csm["NAMA_PRODUK"].unique()))
IFRS_MONTH_csm                          = IFRS_MONTH_filter
PRODUK_csm                              = PRODUK_filter
filtered_csm                            = csm[csm["IFRS_MONTH"] == IFRS_MONTH_csm]
filtered_csm                            = filtered_csm[filtered_csm["NAMA_PRODUK"] == PRODUK_csm]
value_chart                             = filtered_csm['Contractual Service Margin'].tolist()
value_chart                             = [round_school_sig(x, 2) for x in value_chart]

if IFRS_MONTH_filter == "All":
    st.write("PLEASE SELECT A PRODUCT")
else:
    if round_school(sum(value_chart)) == 0:
        st.write("THIS PRODUCT IS ONEROUS")
    else:
        percentages                             = [x / sum(value_chart) * 100 for x in value_chart]
        percentages                             = [round_school_sig(x, 2) for x in percentages]
        st.write(filtered_csm)

        # Sample data
        data = {
            'KEY'           : filtered_csm['KEY'].tolist(),
            'CSM'           : value_chart,
            'Percentage (%)': [f"{p:.2f}" for p in percentages]  # Format percentages
        }

        st.title("COMPARISON OF CSM FROM EACH PRODUCT")

        # Create a plotly pie chart
        fig = px.pie(data, values='CSM', names='KEY', title="comparison of CSM in " + str(IFRS_MONTH_csm))
        st.plotly_chart(fig)

        # Display the table below the chart
        st.subheader("Data Summary")
        st.dataframe(data, use_container_width=True)


    PRODUCT_csm_line    = PRODUK_filter
    KEY_csm_line        = KEY_filter
    filtered_csm_line   = csm[csm["NAMA_PRODUK"] == PRODUCT_csm_line]
    filtered_csm_line   = filtered_csm_line[filtered_csm_line["KEY"] == KEY_csm_line]
    csm_end_value       = filtered_csm_line['Contractual Service Margin'].tolist()
    csm_end_value       = [round_school(x) for x in csm_end_value]

    st.title("CSM MOVEMENT OVER TIME")

    # Create sample data
    data = {
        'IFRS_MONTH': filtered_csm_line['IFRS_MONTH'].tolist(),
        'CSM'       : csm_end_value
    }

    # Create the line chart
    fig = px.line(data, x='IFRS_MONTH', y='CSM', title="CSM MOVEMENT FOR " + PRODUCT_csm_line, markers=True)
    st.plotly_chart(fig)


    # Input Data
    waterfall_df            = csm[csm["IFRS_MONTH"] == IFRS_MONTH_csm]
    waterfall_df            = waterfall_df[waterfall_df["NAMA_PRODUK"] == PRODUCT_csm_line]
    waterfall_df            = waterfall_df[waterfall_df["KEY"] == KEY_csm_line]
    waterfall_df            = waterfall_df.reset_index(drop=True)

    labels = csm.columns
    labels = labels[2:]
    values = [waterfall_df.loc[0, 'Contractual Service Margin_Begin'], waterfall_df.loc[0, 'Kontrak_Baru_Periode_Berjalan'], 
            waterfall_df.loc[0, 'Accretion_Interest_unwind'], waterfall_df.loc[0, 'Efek_Perubahan_Varians_n_Asumsi_Ekonomi'],  
            waterfall_df.loc[0, 'Amortisasi_CSM_Release'], waterfall_df.loc[0, 'Contractual Service Margin']]

    # Create Waterfall Chart
    fig = go.Figure(go.Waterfall(
        orientation="v",
        measure=["relative"] * (len(values) - 1) + ["total"],
        x=labels,
        y=values,
        text=['{:,.2f}'.format(v) for v in values],  # Add values as text on the bars
        textposition="outside",         # Position text outside the bars
        connector={"line": {"color": "rgba(63, 63, 63, 0.5)"}},
    ))

    fig.update_layout(
        title="Waterfall Chart - CSM Movement in " + str(IFRS_MONTH_csm),
        showlegend=False,
        template="plotly_white"
    )

    # Display Chart
    st.plotly_chart(fig)
