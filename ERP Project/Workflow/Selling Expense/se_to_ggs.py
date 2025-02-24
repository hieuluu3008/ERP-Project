import os
import re
import sys
import pandas as pd
import pygsheets
import psycopg2
import datetime
from sqlalchemy import create_engine
import logging


host = 'host_name'
name = 'account_name'
passwd = 'password'
db = 'database_name'
json_path = 'json_path'
###

def QueryPostgre(sql, host, passwd, name, db):
    logging.info("Start db connection")
    db_connection = create_engine(f'postgresql://{name}:{passwd}@{host}:5432/{db}')
    with db_connection.connect() as con, con.begin():
        df = pd.read_sql(sql, con)
        logging.info("Finish querying the data")
    return df

class Gsheet_working:
    def __init__(self, spreadsheet_key, sheet_name, json_file):
        self.spreadsheet_key = spreadsheet_key
        self.sheet_name = sheet_name
        gc = pygsheets.authorize(service_file=json_file)
        sh = gc.open_by_key(spreadsheet_key)
        self.wks = sh.worksheet_by_title(sheet_name)   

    def Update_dataframe(self, dataframe, row_num, col_num, clear_sheet=True, copy_head=True,empty_value=''):
        wks = self.wks
        sheet_name = self.sheet_name
        if clear_sheet:
            print('clear all data in %s'%sheet_name)
            wks.clear()
        else:
            pass

        total_rows = int(len(dataframe))
        print('Start upload data to %s' % sheet_name)
        if total_rows >= 1:
            dataframe.fillna('', inplace=True)
            wks.set_dataframe(dataframe, (row_num, col_num), copy_head=copy_head,nan=empty_value)
            print('Upload successful {} lines'.format(total_rows))
        else:
            print('%s not contain value. Check again' % sheet_name)

# ## SE
spreadsheet_key = '18MD1zFSbFkjfYCNBe0vqjHJXi4XSz_yA6Ks182staV0'


## SE Accrual Est


script = '''SELECT * FROM y4a_erp.y4a_erp_sel_exp_acc_est
WHERE posting_date >= '2025-01-01'
and external_document_no is not null
'''
si = QueryPostgre(sql= script , host=host, passwd=passwd, name=name, db=db)
si_to_gg = Gsheet_working(spreadsheet_key, 'Accural est', json_file = json_path)
si_to_gg.Update_dataframe(si, row_num = 1, col_num = 1, clear_sheet=True, copy_head = True )


# ## Accural Promotion Spends


# script = '''select
#   posting_date,
#   report_month,
#   coupon_id,
#   list_invoice_number,
#   platform,
#   country,
#   platform_prd_id,
#   est_amt_usd,
#   est_amt_lcy,
#   invoiced_amt_usd,
#   invoiced_amt_lcy,
#   accrual_amt_usd,
#   accrual_amt_lcy,
#   run_time,
#   external_doc_no
# from
#   y4a_erp.tb_pnl_sellin_acr_pro_cpn
# where
#   to_char(posting_date, 'YYYY-MM') = '2025-01'
#     AND run_time =(SELECT max(run_time) FROM y4a_erp.tb_pnl_sellin_acr_pro_cpn);'''
# si = QueryPostgre(sql= script , host=host, passwd=passwd, name=name, db=db)
# si_to_gg = Gsheet_working(spreadsheet_key, 'accrual_promotion', json_file = json_path)
# si_to_gg.Update_dataframe(si, row_num = 1, col_num = 1, clear_sheet=True, copy_head = True )


# ## Accrual AMZ Ads Spend


# script = '''select *
# from
#   y4a_erp.tb_pnl_sellin_amz_acr_ads
# where
#   to_char(posting_date, 'YYYY-MM') = '2025-01'
# AND run_time =(SELECT max(run_time) FROM y4a_erp.tb_pnl_sellin_amz_acr_ads);'''
# si = QueryPostgre(sql= script , host=host, passwd=passwd, name=name, db=db)
# si_to_gg = Gsheet_working(spreadsheet_key, 'ERP_AMZ_data_to_AC', json_file = json_path)
# si_to_gg.Update_dataframe(si, row_num = 1, col_num = 1, clear_sheet=True, copy_head = True )
