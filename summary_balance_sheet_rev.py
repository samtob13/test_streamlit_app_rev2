import pandas as pd
import numpy as np
import datetime
import math
import copy
import timeit
import itertools
from sqlalchemy import create_engine
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# Set the page configuration

engine                      = create_engine(f'oracle+oracledb://ACTUARIAL:ACTUARIAL@10.10.0.51:1521/?service_name=RLPROD', thick_mode=None)
qry_balance_sheet           = f"select * from eka.mst_balance_sheet_ifrs_gen"
# qry_balance_sheet           = f"select * from eka.mst_balance_sheet_ifrs_gen where NAMA_PRODUK='SPUL'"
df_balance_sheet            = pd.read_sql(qry_balance_sheet, engine)
df_balance_sheet.columns    = map(str.upper, df_balance_sheet.columns)

base_dir                                = "C:/Users/putra.samuel/PycharmProjects/pythonProject/streamlit_output_rev/"

liability_asset_2023_12                 = pd.read_csv(base_dir + "liability_asset_2023_12.csv")
liability_asset_2023_12['IFRS_MONTH']   = pd.to_datetime(liability_asset_2023_12['IFRS_MONTH'], errors='coerce', format='%m/%d/%Y')
df_balance_sheet['IFRS_MONTH']          = pd.to_datetime(df_balance_sheet['IFRS_MONTH'], errors='coerce', format='%Y-%m-%d')
df_balance_sheet                        = pd.merge(df_balance_sheet, liability_asset_2023_12[["POL_REMARK", "BEL_END_2023_12", "RA_END_2023_12", "CSM_END_2023_12", 
                                                                                              "LIABILITY_2023_12", "ASSET_2023_12", "IFRS_MONTH"]], on=['POL_REMARK', 'IFRS_MONTH'], how='left')
product_type                            = pd.read_csv(base_dir + "type_of_product.csv")
df_balance_sheet                        = pd.merge(df_balance_sheet, product_type, on='NAMA_PRODUK', how='left')
# df_balance_sheet.to_csv(base_dir + "data_balance_sheet.csv", index=False)



# create summary table
balance_sheet_summary                                          = df_balance_sheet.groupby(by=["IFRS_MONTH", "NAMA_PRODUK", "TYPE"]).sum().reset_index()
df                                                             = pd.DataFrame()
df['IFRS_MONTH']                                               = balance_sheet_summary['IFRS_MONTH']
df['NAMA_PRODUK']                                              = balance_sheet_summary['NAMA_PRODUK']
df['TYPE']                                                     = balance_sheet_summary['TYPE'] 
df['Initial_Recognition_PVFCF_Acquisition_Expense']            = balance_sheet_summary['PV_COMMISSION']
df['Initial_Recognition_PVFCF_Maintenance_Expense']            = balance_sheet_summary['PV_EXPENSES']
df['Initial_Recognition_PVFCF_Death_Claim']                    = balance_sheet_summary['PV_CLAIM']
df['PEND_PREMI_GROUP(SINGLE)_CREDIT_LIFE']                     = balance_sheet_summary['PV_CASHFLOW_IN']
df['Initial_Recognition_PVFCF_Onerous']                        = -balance_sheet_summary['INITIAL_LOSS']
df['Initial_Risk_Adjustment']                                  = balance_sheet_summary['RA_INITIAL']
df['ONEROUS_CONTRACT']                                         = -balance_sheet_summary['INITIAL_LOSS']
df['Initial_CSM']                                              = balance_sheet_summary['CSM_INITIAL']
df['CAD_STABLE_LINK_BEL']                                      = balance_sheet_summary['BEL_INITIAL']
df['CAD_STABLE_LINK_RA']                                       = balance_sheet_summary['RA_INITIAL']
df['CAD_STABLE_LINK_CSM']                                      = balance_sheet_summary['CSM_INITIAL']
df['CAD_STABLE_LINK_BEL_current']                              = -(balance_sheet_summary['M_BEL_COMMISSION'] + balance_sheet_summary['M_BEL_ACQ_EXPENSE'] - 
                                                                           (balance_sheet_summary['M_BEL_COMMISSION'] + balance_sheet_summary['M_BEL_ACQ_EXPENSE']) - 
                                                                           (balance_sheet_summary['AMORTITATION_COMM'] + balance_sheet_summary['AMORTITATION_ACQ_COST']) + 
                                                                           balance_sheet_summary['M_BEL_MAINT_EXPENSE'] + balance_sheet_summary['M_BEL_DEATH_CLAIM'] + 
                                                                           balance_sheet_summary['M_BEL_ACC_CLAIM'] + balance_sheet_summary['M_BEL_MORB_CLAIM'] + 
                                                                           balance_sheet_summary['M_BEL_STAGE_CLAIM'] + balance_sheet_summary['M_BEL_SURRENDER_CLAIM'] + 
                                                                           balance_sheet_summary['M_BEL_MATURITY_CLAIM'] + balance_sheet_summary['PREMIUM'] + 
                                                                           balance_sheet_summary['M_BEL_INTEREST'] + balance_sheet_summary['M_BEL_CHANGE_DISC_RATE'] - 
                                                                           (balance_sheet_summary['M_BEL_DEATH_CLAIM'] + balance_sheet_summary['EXP_DEATH_CLAIM']) - 
                                                                           (balance_sheet_summary['M_BEL_ACC_CLAIM'] + balance_sheet_summary['EXP_ACC_CLAIM']) - 
                                                                           (balance_sheet_summary['M_BEL_MORB_CLAIM'] + balance_sheet_summary['EXP_MORB_CLAIM']) - 
                                                                           (balance_sheet_summary['M_BEL_STAGE_CLAIM'] + balance_sheet_summary['EXP_STAGE_CLAIM']) - 
                                                                           (balance_sheet_summary['M_BEL_SURRENDER_CLAIM'] + balance_sheet_summary['EXP_SURRENDER_CLAIM']) - 
                                                                           (balance_sheet_summary['M_BEL_MATURITY_CLAIM'] + balance_sheet_summary['EXP_MATURITY_CLAIM']) - 
                                                                           (balance_sheet_summary['M_BEL_MAINT_EXPENSE'] + balance_sheet_summary['EXP_EXPENSE']) + 
                                                                           balance_sheet_summary['REVERSE_LOSS'])
df['CAD_STABLE_LINK_RA_current']                               = -(balance_sheet_summary['M_RA_RELEASE'] + balance_sheet_summary['M_RA_INTEREST'] + 
                                                                    balance_sheet_summary['M_RA_CHANGE_DISC_RATE'] - 
                                                                    (balance_sheet_summary['M_RA_RELEASE'] + balance_sheet_summary['EXP_RELEASE_RA']))
df['CAD_STABLE_LINK_CSM_current']                              = -(balance_sheet_summary['M_CSM_RELEASE_CSM'] + balance_sheet_summary['M_CSM_INTEREST'] - 
                                                                    (balance_sheet_summary['CHANGE_IN_PV_I_BEL'] + balance_sheet_summary['CHANGE_IN_PV_I_RA']))
df['Subsequent_Changes_PVFCF_Commission']                      = balance_sheet_summary['M_BEL_COMMISSION'] + balance_sheet_summary['M_BEL_ACQ_EXPENSE']
df['Subsequent_Changes_PVFCF_Commission_reversed']             = -(balance_sheet_summary['M_BEL_COMMISSION'] + balance_sheet_summary['M_BEL_ACQ_EXPENSE'])
df['Subsequent_Changes_PVFCF_Amortised_Acquisition_Expense']   = -(balance_sheet_summary['AMORTITATION_COMM'] + balance_sheet_summary['AMORTITATION_ACQ_COST'])
df['Subsequent_Changes_PVFCF_Maintenance_Expense']             = balance_sheet_summary['M_BEL_MAINT_EXPENSE']
df['Subsequent_Changes_PVFCF_Death_Claim']                     = balance_sheet_summary['M_BEL_DEATH_CLAIM']
df['Subsequent_Changes_PVFCF_Accident_Claim']                  = balance_sheet_summary['M_BEL_ACC_CLAIM']
df['Subsequent_Changes_PVFCF_Morbidty_Claim']                  = balance_sheet_summary['M_BEL_MORB_CLAIM']
df['Subsequent_Changes_PVFCF_Stage_Claim']                     = balance_sheet_summary['M_BEL_STAGE_CLAIM']
df['Subsequent_Changes_PVFCF_Surrender_Claim']                 = balance_sheet_summary['M_BEL_SURRENDER_CLAIM']
df['Subsequent_Changes_PVFCF_Maturity_Claim']                  = balance_sheet_summary['M_BEL_MATURITY_CLAIM']
df['Subsequent_Changes_PVFCF_Premium_Single']                  = balance_sheet_summary['PREMIUM']

df['Subsequent_Changes_RA']                                    = balance_sheet_summary['M_RA_RELEASE']
df['Release_in_CSM']                                           = balance_sheet_summary['M_CSM_RELEASE_CSM']
df['Interest_Accretion_PVFCF']                                 = balance_sheet_summary['M_BEL_INTEREST']
df['Interest_Accretion_RA']                                    = balance_sheet_summary['M_RA_INTEREST']
df['Interest_Accretion_CSM']                                   = balance_sheet_summary['M_CSM_INTEREST']
df['Interest_Accretion_change_in_pv']                          = -(balance_sheet_summary['CHANGE_IN_PV_I_BEL'] + balance_sheet_summary['CHANGE_IN_PV_I_RA'])
df['Change_in_Discount_Rate_PVFCF']                            = balance_sheet_summary['M_BEL_CHANGE_DISC_RATE']
df['Change_in_Discount_Rate_RA']                               = balance_sheet_summary['M_RA_CHANGE_DISC_RATE']

df['Subsequent_Changes_PVFCF_Death_Claim_exp']                 = -(balance_sheet_summary['M_BEL_DEATH_CLAIM'] + balance_sheet_summary['EXP_DEATH_CLAIM'])
df['Subsequent_Changes_PVFCF_Accident_Claim_exp']              = -(balance_sheet_summary['M_BEL_ACC_CLAIM'] + balance_sheet_summary['EXP_ACC_CLAIM'])
df['Subsequent_Changes_PVFCF_Morbidty_Claim_exp']              = -(balance_sheet_summary['M_BEL_MORB_CLAIM'] + balance_sheet_summary['EXP_MORB_CLAIM'])
df['Subsequent_Changes_PVFCF_Stage_Claim_exp']                 = -(balance_sheet_summary['M_BEL_STAGE_CLAIM'] + balance_sheet_summary['EXP_STAGE_CLAIM'])
df['Subsequent_Changes_PVFCF_Surrender_Claim_exp']             = -(balance_sheet_summary['M_BEL_SURRENDER_CLAIM'] + balance_sheet_summary['EXP_SURRENDER_CLAIM'])
df['Subsequent_Changes_PVFCF_Maturity_Claim_exp']              = -(balance_sheet_summary['M_BEL_MATURITY_CLAIM'] + balance_sheet_summary['EXP_MATURITY_CLAIM'])
df['Subsequent_Changes_PVFCF_Maintenance_Expense_exp']         = -(balance_sheet_summary['M_BEL_MAINT_EXPENSE'] + balance_sheet_summary['EXP_EXPENSE'])
df['Subsequent_Changes_RA_exp']                                = -(balance_sheet_summary['M_RA_RELEASE'] + balance_sheet_summary['EXP_RELEASE_RA'])

df['REVERSAL_LOSS_ONEROUS_CONTRACT_ON_ASURANSI_PERORANGAN']    = balance_sheet_summary['REVERSE_LOSS']
df['AMORTISASI_BIAYA_AKUISISI_DITANGGUHKAN_IND']               = (balance_sheet_summary['AMORTITATION_COMM'] + balance_sheet_summary['AMORTITATION_ACQ_COST'])
df['DAC_TAHUN_PERTAMA_(RP)']                                   = -(balance_sheet_summary['AMORTITATION_COMM'] + balance_sheet_summary['AMORTITATION_ACQ_COST'])
df['CAD_STABLE_LINK_Onerous_current']                          = -(balance_sheet_summary['INTEREST_ACCRETION_LC'] + balance_sheet_summary['ADJUSTMENT_INITIAL_LOSS'])
df['INTEREST_ACCRETION_LC_PVFCF']                              = (balance_sheet_summary['M_BEL_INTEREST_FULL'] + balance_sheet_summary['M_BEL_INTEREST'])
df['INTEREST_ACCRETION_LC_RA']                                 = (balance_sheet_summary['M_RA_INTEREST_FULL'] + balance_sheet_summary['M_RA_INTEREST'])
df['ADJUSTMENT_INITIAL_LOSS']                                  = balance_sheet_summary['ADJUSTMENT_INITIAL_LOSS'] 

df['BEL_END_2023_12']               = balance_sheet_summary['BEL_END_2023_12']
df['RA_END_2023_12']                = balance_sheet_summary['RA_END_2023_12']
df['CSM_END_2023_12']               = balance_sheet_summary['CSM_END_2023_12']
df['LIABILITY_2023_12']             = balance_sheet_summary['LIABILITY_2023_12']
df['ASSET_2023_12']                 = balance_sheet_summary['ASSET_2023_12']

df['Movement_Liability']            = df['CAD_STABLE_LINK_BEL_current'] + df['CAD_STABLE_LINK_RA_current'] + df['CAD_STABLE_LINK_CSM_current'] + df['CAD_STABLE_LINK_Onerous_current']
df.loc[0, 'Movement_Liability']     = df.loc[0, 'Movement_Liability'] + df.loc[0, 'CAD_STABLE_LINK_BEL'] + df.loc[0, 'CAD_STABLE_LINK_RA'] + df.loc[0, 'CAD_STABLE_LINK_CSM'] + df.loc[0, 'ONEROUS_CONTRACT']
df['Movement_Asset']                = df['Subsequent_Changes_PVFCF_Commission_reversed'] + df['Subsequent_Changes_PVFCF_Amortised_Acquisition_Expense']
df['Movement_RA']                   = balance_sheet_summary['RA_INITIAL'] + balance_sheet_summary['M_RA_INTEREST'] + balance_sheet_summary['M_RA_CHANGE_DISC_RATE'] - balance_sheet_summary['EXP_RELEASE_RA'] + balance_sheet_summary['M_RA_CHANGE_IN_PV']

product_list                        = df['NAMA_PRODUK'].tolist()
product_list                        = np.unique(product_list)
df_i                                = df.drop(df[df.NAMA_PRODUK != product_list[0]].index)
df_i.loc[0, 'Movement_Liability']   = 0
df_i.loc[0, 'Movement_Asset']       = 0
df_i['Liability']                   = np.cumsum(df_i['Movement_Liability'].to_numpy())
df_i['Asset']                       = np.cumsum(df_i['Movement_Asset'].to_numpy())
df_i['Risk_Adjustment']             = np.cumsum(df_i['Movement_RA'].to_numpy())

for x in range(1, len(product_list)):
    df_j                                = df.drop(df[df.NAMA_PRODUK != product_list[x]].index)
    df_j.loc[0, 'Movement_Liability']   = 0
    df_j.loc[0, 'Movement_Asset']       = 0
    df_j['Liability']                   = np.cumsum(df_j['Movement_Liability'].to_numpy())
    df_j['Asset']                       = np.cumsum(df_j['Movement_Asset'].to_numpy())
    df_j['Risk_Adjustment']             = np.cumsum(df_j['Movement_RA'].to_numpy())
    df_i                                = pd.concat([df_i, df_j], ignore_index=True)

df                                                  = df_i
Liabilitas_Kontrak_Asuransi_Saldo_Awal              = df['Liability'].tolist()[-1:] + df['Liability'].tolist()[:-1]
Liabilitas_Kontrak_Asuransi_Saldo_Awal              = [-x for x in Liabilitas_Kontrak_Asuransi_Saldo_Awal]
Aset_Kontrak_Asuransi_Saldo_Awal                    = df['Asset'].tolist()[-1:] + df['Asset'].tolist()[:-1]
Saldo_Bersih_Kontrak_Asuransi_Saldo_Awal            = [x - y for x, y in zip(Liabilitas_Kontrak_Asuransi_Saldo_Awal, Aset_Kontrak_Asuransi_Saldo_Awal)]

df['Liabilitas_Kontrak_Asuransi_Saldo_Awal']        = Liabilitas_Kontrak_Asuransi_Saldo_Awal
df['Aset_Kontrak_Asuransi_Saldo_Awal']              = Aset_Kontrak_Asuransi_Saldo_Awal
df['Saldo_Bersih_Kontrak_Asuransi_Saldo_Awal']      = Saldo_Bersih_Kontrak_Asuransi_Saldo_Awal

balance_sheet_summary                               = pd.merge(balance_sheet_summary, df[["IFRS_MONTH" ,"NAMA_PRODUK", "Liabilitas_Kontrak_Asuransi_Saldo_Awal", "Aset_Kontrak_Asuransi_Saldo_Awal", "Saldo_Bersih_Kontrak_Asuransi_Saldo_Awal"]], on=['NAMA_PRODUK', 'IFRS_MONTH'], how='left')

## Pendapatan Jasa Asuransi ##
Ekspektasi_Klaim                                    = [a + b + c + d + e + f + g + h + i for a, b, c, d, e, f, g, h, i in 
                                                        zip(balance_sheet_summary['EXP_DEATH_CLAIM'].tolist(), balance_sheet_summary['EXP_ACC_CLAIM'].tolist(), balance_sheet_summary['EXP_MORB_CLAIM'].tolist(), 
                                                            balance_sheet_summary['EXP_STAGE_CLAIM'].tolist(), balance_sheet_summary['EXP_SURRENDER_CLAIM'].tolist(), balance_sheet_summary['EXP_MATURITY_CLAIM'].tolist(), 
                                                            balance_sheet_summary['EXP_HCP_CLAIM'].tolist(), balance_sheet_summary['EXP_HCP_PLUS_CLAIM'].tolist(), balance_sheet_summary['EXP_CI_CLAIM'].tolist())]

Ekspektasi_Komisi                                   = [a + b for a, b in zip(balance_sheet_summary['AMORTITATION_COMM'].tolist(), balance_sheet_summary['AMORTITATION_ACQ_COST'].tolist())]
Ekspektasi_Beban_Attributable                       = balance_sheet_summary['EXP_EXPENSE'].tolist()
Ekspektasi_Klaim_n_Beban_Attributable               = [(x + y + z) for x, y, z in zip(Ekspektasi_Klaim, Ekspektasi_Komisi, Ekspektasi_Beban_Attributable)]
Perubahan_pada_Penyesuaian_Risiko_RA                = balance_sheet_summary['EXP_RELEASE_RA'].tolist()
Perubahan_pada_CSM_Amortisasi                       = balance_sheet_summary['M_CSM_RELEASE_CSM'].tolist()
Perubahan_pada_CSM_Amortisasi                       = [-x for x in Perubahan_pada_CSM_Amortisasi]
Pendapatan_Jasa_Asuransi_Lainnya                    = [0] * len(balance_sheet_summary)
Pendapatan_Jasa_Asuransi                            = [w + x + y + z for w, x, y, z in zip(Ekspektasi_Klaim_n_Beban_Attributable, Perubahan_pada_Penyesuaian_Risiko_RA, Perubahan_pada_CSM_Amortisasi, Pendapatan_Jasa_Asuransi_Lainnya)]

## Beban Jasa Asuransi ##
Beban_Klaim_n_Biaya_Attributable                    = [-x for x in balance_sheet_summary['M_BEL_MAINT_EXPENSE'].tolist()]
Amortisasi_Arus_Kas_Akuisisi_Beban                  = [x for x in Ekspektasi_Komisi]
Kerugian_dari_Kontrak_Merugi_Onerous                = [x + y + z for x, y, z in zip(balance_sheet_summary['ADJUSTMENT_INITIAL_LOSS'].tolist(), balance_sheet_summary['INITIAL_LOSS'].tolist(), balance_sheet_summary['REVERSE_LOSS'].tolist())]
Beban_Jasa_Asuransi_Lainnya                         = [0] * len(balance_sheet_summary)
Beban_Jasa_Asuransi                                 = [w + x + y + z for w, x, y, z in zip(Beban_Klaim_n_Biaya_Attributable, Amortisasi_Arus_Kas_Akuisisi_Beban, Kerugian_dari_Kontrak_Merugi_Onerous, Beban_Jasa_Asuransi_Lainnya)]
Hasil_Jasa_Asuransi_Kotor                           = [x - y for x, y in zip(Beban_Jasa_Asuransi, Pendapatan_Jasa_Asuransi)]

## Arus_Kas_Selama_Periode ##
type_list                                           = balance_sheet_summary['TYPE'].tolist()
Penerimaan_Premi                                    = balance_sheet_summary['PREMIUM'].tolist()
Penerimaan_Premi_UL                                 = [a + b + c + d - e - f + g for a, b, c, d, e, f, g in
                                                        zip(balance_sheet_summary['COST_OF_INSURANCE'].tolist(), balance_sheet_summary['COST_OF_INSURANCE_HCP'].tolist(), 
                                                            balance_sheet_summary['COST_OF_INSURANCE_HCP_PLUS'].tolist(), balance_sheet_summary['COST_OF_INSURANCE_CI'].tolist(), 
                                                            balance_sheet_summary['MANAGEMENT_FEE_FUND'].tolist(), balance_sheet_summary['ADMIN_FEE_FUND'].tolist(), 
                                                            balance_sheet_summary['SURRENDER_FEE'].tolist())]
cash_UL                                             = [a + b + c + d + e + f for a, b, c, d, e, f in
                                                        zip(balance_sheet_summary['COST_OF_INSURANCE'].tolist(), balance_sheet_summary['COST_OF_INSURANCE_HCP'].tolist(), 
                                                            balance_sheet_summary['COST_OF_INSURANCE_HCP_PLUS'].tolist(), balance_sheet_summary['COST_OF_INSURANCE_CI'].tolist(), 
                                                            balance_sheet_summary['MANAGEMENT_FEE'].tolist(), balance_sheet_summary['SURRENDER_FEE'].tolist())]            
Penerimaan_Premi                                    = [x if y == "TRADITIONAL" else z for x, y, z in zip(Penerimaan_Premi, type_list, Penerimaan_Premi_UL)]

Arus_Kas_Akuisisi_Asuransi                          = [x + y for x, y in zip(balance_sheet_summary['M_BEL_COMMISSION'].tolist(), balance_sheet_summary['M_BEL_ACQ_EXPENSE'].tolist())]
Beban_Klaim_n_Beban_Jasa_Asuransi_Dibayarkan        = [0] * len(balance_sheet_summary)
Arus_Kas_Bersih                                     = [x - y - z for x, y, z in zip(Penerimaan_Premi, Arus_Kas_Akuisisi_Asuransi, Beban_Klaim_n_Beban_Jasa_Asuransi_Dibayarkan)]
Pendapatan_Beban_Keuangan_Kontrak_Asuransi_PnL      = [-x + y + z for x, y, z in zip(Pendapatan_Jasa_Asuransi, Kerugian_dari_Kontrak_Merugi_Onerous, Penerimaan_Premi)]
Pendapatan_Beban_Keuangan_Kontrak_Asuransi_OCI      = [0] * len(balance_sheet_summary)
Alokasi_Aset_untuk_Arus_Kas_Akuisisi                = [(x + y) for x, y in zip(Arus_Kas_Akuisisi_Asuransi, Amortisasi_Arus_Kas_Akuisisi_Beban)]
Komponen_Investasi                                  = [a + b + c + d + e for a, b, c, d, e in
                                                        zip(balance_sheet_summary['M_CSM_INTEREST'].tolist(), balance_sheet_summary['M_BEL_INTEREST_FULL'].tolist(), balance_sheet_summary['M_RA_INTEREST_FULL'].tolist(), 
                                                            balance_sheet_summary['M_BEL_CHANGE_DISC_RATE'].tolist(), balance_sheet_summary['M_RA_CHANGE_DISC_RATE'].tolist())]
Komponen_Investasi                                  = [w - (x + y) if z == "TRADITIONAL" else w for w, x, y, z in zip(Komponen_Investasi, 
                                                                                                                      balance_sheet_summary['CHANGE_IN_PV_I_BEL'].tolist(), balance_sheet_summary['CHANGE_IN_PV_I_RA'].tolist(), 
                                                                                                                      type_list)]
Perubahan_Lainnya                                   = [0 if x == "TRADITIONAL" else y - z for x, y, z in zip(type_list, cash_UL, Penerimaan_Premi_UL)]
Total_Perubahan                                     = [v + w + x + y + z for v, w, x, y, z in
                                                        zip(Pendapatan_Beban_Keuangan_Kontrak_Asuransi_PnL, Pendapatan_Beban_Keuangan_Kontrak_Asuransi_OCI, 
                                                            Alokasi_Aset_untuk_Arus_Kas_Akuisisi, Komponen_Investasi, Perubahan_Lainnya)]

Liabilitas_Kontrak_Asuransi_Saldo_Awal              = balance_sheet_summary['Liabilitas_Kontrak_Asuransi_Saldo_Awal'].tolist()
Aset_Kontrak_Asuransi_Saldo_Awal                    = balance_sheet_summary['Aset_Kontrak_Asuransi_Saldo_Awal'].tolist()
Saldo_Bersih_Kontrak_Asuransi_Saldo_Awal            = balance_sheet_summary['Saldo_Bersih_Kontrak_Asuransi_Saldo_Awal'].tolist()

Aset_Kontrak_Asuransi_Saldo_Akhir                   = [x - y for x, y in zip(Aset_Kontrak_Asuransi_Saldo_Awal, Alokasi_Aset_untuk_Arus_Kas_Akuisisi)]
Liabilitas_Kontrak_Asuransi_Saldo_Akhir             = [x + y + z for x, y, z in zip(Saldo_Bersih_Kontrak_Asuransi_Saldo_Awal, Total_Perubahan, Aset_Kontrak_Asuransi_Saldo_Akhir)]
Saldo_Bersih_Kontrak_Asuransi_Saldo_Akhir           = [x - y for x, y in zip(Liabilitas_Kontrak_Asuransi_Saldo_Akhir, Aset_Kontrak_Asuransi_Saldo_Akhir)]

reconcile                                                       = pd.DataFrame()
reconcile['IFRS_MONTH']                                         = balance_sheet_summary['IFRS_MONTH'] 
reconcile['NAMA_PRODUK']                                        = balance_sheet_summary['NAMA_PRODUK']
reconcile['Liabilitas Kontrak Asuransi (Saldo Awal)']           = Liabilitas_Kontrak_Asuransi_Saldo_Awal
reconcile['Aset Kontrak Asuransi (Saldo Awal)']                 = Aset_Kontrak_Asuransi_Saldo_Awal
reconcile['Saldo Bersih Kontrak Asuransi (Saldo Awal)']         = Saldo_Bersih_Kontrak_Asuransi_Saldo_Awal
reconcile['Pendapatan Jasa Asuransi']                           = Pendapatan_Jasa_Asuransi
reconcile['Ekspektasi Klaim & Beban Attributable (Estimasi)']   = Ekspektasi_Klaim_n_Beban_Attributable
reconcile['Perubahan pada Penyesuaian Risiko (RA)']             = Perubahan_pada_Penyesuaian_Risiko_RA
reconcile['Perubahan pada CSM (Amortisasi)']                    = Perubahan_pada_CSM_Amortisasi
reconcile['Pendapatan Jasa Asuransi Lainnya']                   = Pendapatan_Jasa_Asuransi_Lainnya
reconcile['Beban Jasa Asuransi']                                = Beban_Jasa_Asuransi
reconcile['Beban Klaim & Biaya Attributable']                   = Beban_Klaim_n_Biaya_Attributable
reconcile['Amortisasi Arus Kas Akuisisi (Beban)']               = Amortisasi_Arus_Kas_Akuisisi_Beban
reconcile['Kerugian dari Kontrak Merugi (Onerous)']             = Kerugian_dari_Kontrak_Merugi_Onerous
reconcile['Beban Jasa Asuransi Lainnya']                        = Beban_Jasa_Asuransi_Lainnya
reconcile['Hasil Jasa Asuransi Kotor']                          = Hasil_Jasa_Asuransi_Kotor
reconcile['Penerimaan Premi']                                   = Penerimaan_Premi
reconcile['Arus Kas Akuisisi Asuransi']                         = Arus_Kas_Akuisisi_Asuransi
reconcile['Beban Klaim & Beban Jasa Asuransi Dibayarkan']       = Beban_Klaim_n_Beban_Jasa_Asuransi_Dibayarkan
reconcile['Arus Kas Bersih']                                    = Arus_Kas_Bersih
reconcile['Pendapatan (Beban) Keuangan Kontrak Asuransi (P&L)'] = Pendapatan_Beban_Keuangan_Kontrak_Asuransi_PnL
reconcile['Pendapatan (Beban) Keuangan Kontrak Asuransi (OCI)'] = Pendapatan_Beban_Keuangan_Kontrak_Asuransi_OCI
reconcile['Alokasi Aset untuk Arus Kas Akuisisi']               = Alokasi_Aset_untuk_Arus_Kas_Akuisisi
reconcile['Komponen Investasi']                                 = Komponen_Investasi
reconcile['Perubahan Lainnya']                                  = Perubahan_Lainnya
reconcile['Total Perubahan']                                    = Total_Perubahan
reconcile['Liabilitas Kontrak Asuransi (Saldo Akhir)']          = Liabilitas_Kontrak_Asuransi_Saldo_Akhir
reconcile['Aset Kontrak Asuransi (Saldo Akhir)']                = Aset_Kontrak_Asuransi_Saldo_Akhir
reconcile['Saldo Bersih Kontrak Asuransi (Saldo Akhir)']        = Saldo_Bersih_Kontrak_Asuransi_Saldo_Akhir
reconcile['BEL_END_2023_12']                                    = balance_sheet_summary['BEL_END_2023_12']
reconcile['RA_END_2023_12']                                     = balance_sheet_summary['RA_END_2023_12']
reconcile['CSM_END_2023_12']                                    = balance_sheet_summary['CSM_END_2023_12']
reconcile['LIABILITY_2023_12']                                  = balance_sheet_summary['LIABILITY_2023_12']
reconcile['ASSET_2023_12']                                      = balance_sheet_summary['ASSET_2023_12']
reconcile['Kontrak_Baru_Periode_Berjalan']                      = balance_sheet_summary['CSM_INITIAL'].tolist()
reconcile['Accretion_Interest_unwind']                          = balance_sheet_summary['M_CSM_INTEREST'].tolist()
reconcile['Efek_Perubahan_Varians_n_Asumsi_Ekonomi']            = [x + y for x, y in zip(balance_sheet_summary['M_CSM_ADJ_FROM_PV'].tolist(), balance_sheet_summary['M_CSM_ADJ_DEVIATION'].tolist())]
reconcile['Amortisasi_CSM_Release']                             = balance_sheet_summary['M_CSM_RELEASE_CSM'].tolist()

reconcile           = reconcile.sort_values(["NAMA_PRODUK", "IFRS_MONTH"], ascending=[True, True])
IFRS_MONTH_list     = reconcile['IFRS_MONTH'].tolist()
NAMA_PRODUK_list    = reconcile['NAMA_PRODUK'].tolist()
index_list          = reconcile.index
cek_list            = [1]

for x in range(1, len(NAMA_PRODUK_list)):
    if NAMA_PRODUK_list[x] == NAMA_PRODUK_list[x - 1]:
        cek_list.append(cek_list[x - 1] + 1)
    else:
        cek_list.append(1)

# reconcile['cek']        = cek_list 
LIABILITY_2023_12_list  = reconcile['LIABILITY_2023_12'].tolist()
Total_Perubahan         = reconcile['Total Perubahan'].tolist()
Total_Perubahan         = [0 if x == 1 and y == datetime.datetime(2023, 12, 1) else z for x, y, z in 
                           zip(cek_list, IFRS_MONTH_list, Total_Perubahan)]

ASSET_2023_12_list      = reconcile['ASSET_2023_12'].tolist()
Alokasi_Aset            = reconcile['Alokasi Aset untuk Arus Kas Akuisisi'].tolist()
Alokasi_Aset            = [-x for x in Alokasi_Aset]
Alokasi_Aset            = [0 if x == 1 and y == datetime.datetime(2023, 12, 1) else z for x, y, z in 
                           zip(cek_list, IFRS_MONTH_list, Alokasi_Aset)]

product_pos             = [NAMA_PRODUK_list.index(x) for x in NAMA_PRODUK_list]
sum_end                 = [x + y for x, y in zip(product_pos, cek_list)]
liabilty                = [sum(Total_Perubahan[x : (x + y)]) for x, y in zip(product_pos, cek_list)]
liabilty                = [LIABILITY_2023_12_list[x] + y for x, y in zip(product_pos, liabilty)]

asset                   = [sum(Alokasi_Aset[x : (x + y)]) for x, y in zip(product_pos, cek_list)]
asset                   = [ASSET_2023_12_list[x] + y for x, y in zip(product_pos, asset)]

Kontrak_Baru_Periode_Berjalan           = reconcile['Kontrak_Baru_Periode_Berjalan'].tolist()
Accretion_Interest_unwind               = reconcile['Accretion_Interest_unwind'].tolist()
Efek_Perubahan_Varians_n_Asumsi_Ekonomi = reconcile['Efek_Perubahan_Varians_n_Asumsi_Ekonomi'].tolist()
Amortisasi_CSM_Release                  = reconcile['Amortisasi_CSM_Release'].tolist()
movement_csm                            = [w + x + y + z for w, x, y, z in zip(Kontrak_Baru_Periode_Berjalan, Accretion_Interest_unwind, Efek_Perubahan_Varians_n_Asumsi_Ekonomi, Amortisasi_CSM_Release)]

CSM_END_2023_12_list    = reconcile['CSM_END_2023_12'].tolist()
movement_csm            = [0 if x == 1 and y == datetime.datetime(2023, 12, 1) else z for x, y, z in zip(cek_list, IFRS_MONTH_list, movement_csm)]
csm                     = [sum(movement_csm[x : (x + y)]) for x, y in zip(product_pos, cek_list)]
csm                     = [CSM_END_2023_12_list[x] + y for x, y in zip(product_pos, csm)]

reconcile['Saldo Bersih Kontrak Asuransi (Saldo Akhir)']    = liabilty
reconcile['Aset Kontrak Asuransi (Saldo Akhir)']            = asset
reconcile['Liabilitas Kontrak Asuransi (Saldo Akhir)']      = [x + y for x, y in zip(liabilty, asset)]

reconcile['Saldo Bersih Kontrak Asuransi (Saldo Awal)']     = reconcile['Saldo Bersih Kontrak Asuransi (Saldo Akhir)'] - reconcile['Total Perubahan']
reconcile['Aset Kontrak Asuransi (Saldo Awal)']             = reconcile['Aset Kontrak Asuransi (Saldo Akhir)'] + reconcile['Alokasi Aset untuk Arus Kas Akuisisi']
reconcile['Liabilitas Kontrak Asuransi (Saldo Awal)']       = reconcile['Saldo Bersih Kontrak Asuransi (Saldo Awal)'] + reconcile['Aset Kontrak Asuransi (Saldo Awal)'] 
reconcile['Contractual Service Margin']                     = csm

# reconcile.iloc[:, 0:len(reconcile.columns) - 10].to_csv(base_dir + "balance_sheet_total.csv", index=False)
csm_df  = pd.concat([reconcile.iloc[:, 0:2], reconcile.iloc[:, len(reconcile.columns) - 5:]], axis=1)
# csm_df.to_csv(base_dir + "csm_total.csv", index=False)

scopes      = ['https://spreadsheets.google.com/feeds', 
                   'https://www.googleapis.com/auth/spreadsheets', 
                   'https://www.googleapis.com/auth/drive.file', 
                   'https://www.googleapis.com/auth/drive']

creds       = ServiceAccountCredentials.from_json_keyfile_name("C:\\Users\\putra.samuel\\PycharmProjects\\pythonProject\\data\\readstreamlit-41cda8d43512.json", scopes=scopes)

def connect_to_googlesheet(spreadsheet_name, sheet_name):
    file        = gspread.authorize(credentials=creds)
    workbook    = file.open(spreadsheet_name)
    return workbook.worksheet(sheet_name)

# GMM_VFA_df  = pd.DataFrame(connect_to_googlesheet('balance_sheet_total', 'GMM_VFA').get_all_records())
# PAA_df      = pd.DataFrame(connect_to_googlesheet('balance_sheet_total', 'PAA').get_all_records())
connect_to_googlesheet('balance_sheet_total', 'tes').clear()
set_with_dataframe(connect_to_googlesheet('balance_sheet_total', 'tes'), reconcile.iloc[:, 0:len(reconcile.columns) - 10])
connect_to_googlesheet('balance_sheet_total', 'GMM_VFA').clear()
set_with_dataframe(connect_to_googlesheet('balance_sheet_total', 'GMM_VFA'), reconcile.iloc[:, 0:len(reconcile.columns) - 10])
connect_to_googlesheet('balance_sheet_total', 'CSM').clear()
set_with_dataframe(connect_to_googlesheet('balance_sheet_total', 'CSM'), csm_df)



