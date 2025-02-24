# =========================================================================
# Bat buoc phai copy doan code nay truoc khi xai
import datetime
import os
import re
import sys
import datetime
import pandas as pd
import datetime
import pygsheets
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import logging



# Global variables
host = 'host_name'
name = 'account_name'
passwd = 'password'
db = 'database_name'
json_path = 'json_path'


ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def call_stored_procedure(host, name, passwd, db, schema, procedure_name):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(host=host, user=name, password=passwd, database=db)
   
    try:
        # Create a cursor object to interact with the database
        with conn.cursor() as cursor:
            # Build the SQL query to call the stored procedure without parameters
            query = sql.SQL("CALL {}.{}()").format(sql.Identifier(schema), sql.Identifier(procedure_name))
           
            # Execute the query
            cursor.execute(query)
           
            # Commit the transaction
            conn.commit()
           
            print(f"Stored procedure '{schema}.{procedure_name}' executed successfully.")
   
    except Exception as e:
        print(f"Error calling stored procedure '{schema}.{procedure_name}': {e}")
   
    finally:
        # Close the database connection
        conn.close()


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

# Run procedure to ingest data
procedure_schema = 'y4a_erp'

procedure_name_1 = 'sp_prod_sales_invoice_02_2_wm_wfs_daily'
procedure_name_2 = 'sp_prod_sales_invoice_02_2_wm_dsv_daily'
procedure_name_3 = 'sp_prod_sales_invoice_02_2_wayfair_daily'
procedure_name_4 = 'sp_prod_sales_invoice_02_2_avc_ds_daily'
procedure_name_5 = 'sp_prod_sales_invoice_02_2_asc_daily'
procedure_name_6 = 'sp_prod_sales_invoice_02_2_avc_di_wh_daily'
# procedure_name_7 = 'sp_y4a_erp_sel_exp_cop_inv_api_upt'
# procedure_name_8 = 'sp_y4a_erp_sel_exp_ads_dtl_api_upt'
# procedure_name_9 = 'sp_y4a_erp_sel_exp_inv_api_log'
# procedure_name_10 = 'sp_y4a_erp_sel_exp_inv_api_incr'


call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_1)
call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_2)
call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_3)
call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_4)
call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_5)
call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_6)
# call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_7)
# call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_8)
# call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_9)
# call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_10)


# Run view to QA Check
spreadsheet_key = '1FVdAsXbCynp4cwi0benUIAGPJwK1AEE9mP54dIpIrrU'
# https://docs.google.com/spreadsheets/d/1FVdAsXbCynp4cwi0benUIAGPJwK1AEE9mP54dIpIrrU/edit?gid=0#gid=0


# Define queries
queries = {
    'wm_wfs_daily': 'select * from y4a_erp.view_y4a_erp_sales_invoice_reconcile_wm_wfs_daily',
    'wm_dsv_daily': 'select * from y4a_erp.view_y4a_erp_sales_invoice_reconcile_wm_dsv_daily',
    'wm_dsv_daily_v2': 'select * from y4a_erp.view_y4a_erp_sales_invoice_reconcile_wm_dsv_daily_v2',
    'wm_dsv_transfer_add_in': 'select * from y4a_erp.view_y4a_erp_sales_invoice_reconcile_wm_dsv_transfer_add_in_dai',
    'wf_daily': 'select * from y4a_erp.view_y4a_erp_sales_invoice_reconcile_wf_daily',
    'wf_rcc_si_net_sales_vs_invoice_porta': 'select * from y4a_erp.y4a_erp_view_si_wayfair_reconcile_si_net_sales_vs_invoice_porta',
    'avc_ds_daily': 'select * from y4a_erp.view_y4a_erp_sales_invoice_reconcile_avc_ds_daily',
    'asc_daily': '''select * from y4a_erp.view_y4a_erp_sales_invoice_reconcile_asc_daily where raw_month = (select to_char(max(posting_date)::date,'yyyy-mm') from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily)''',
    'avc_di_wh_daily': 'select * from y4a_erp.view_y4a_erp_sales_invoice_reconcile_avc_di_wh_daily'
}


# Define sheet configurations
sheet_configs = [
    ('language_checking', 'language_checking'),('dim_checking', 'dim_checking'),
    ('wm_wfs_daily', 'wm_wfs_daily'),
    ('wm_dsv_daily', 'wm_dsv_daily'),('wm_dsv_daily_v2', 'wm_dsv_daily_v2'),('wm_dsv_transfer_add_in', 'wm_dsv_transfer_add_in'),
    ('wf_daily', 'wf_daily'),('wf_rcc_si_net_sales_vs_invoice_porta', 'wf_rcc_si_net_sales_vs_invoice_porta'),
    ('avc_ds_daily', 'avc_ds_daily'),
    ('asc_daily', 'asc_daily'),
    ('avc_di_wh_daily', 'avc_di_wh_daily')
]
# Process all queries and update sheets
for query_key, sheet_name in sheet_configs:
    si = QueryPostgre(sql=queries[query_key], host=host, passwd=passwd, name=name, db=db)
    si_to_gg = Gsheet_working(spreadsheet_key, sheet_name, json_file=json_path)
    si_to_gg.Update_dataframe(si, row_num=1, col_num=1, clear_sheet=True, copy_head=True)
