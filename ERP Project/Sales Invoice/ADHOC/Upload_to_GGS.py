import os
import re
import sys
import pandas as pd
import pygsheets
import psycopg2
import datetime
from sqlalchemy import create_engine
import logging
from google.oauth2.service_account import Credentials

host = 'host_name'
name = 'account_name'
passwd = 'password'
db = 'database_name'


def QueryPostgre(sql, host, passwd, name, db):
    logging.info("Start db connection")
    db_connection = create_engine(f'postgresql://{name}:{passwd}@{host}:5432/{db}')
    with db_connection.connect() as con, con.begin():
        df = pd.read_sql(sql, con)
        logging.info("Finish querying the data")
    return df

script = '''
select a.order_date,
 a.posting_date , a.document_date ,
 a.document_type, a.bill_to_customer ,
 a.sell_to_customer , a.platform ,
 a.posting_description , a.payment_term_code,
 a.external_doc_no , a.location ,
 a.currency , a.sales_channel ,
 a.country , a.internal_sales_channel ,
 a.original_external_doc_no , po_number, a.original_y4a_company_id ,
 b.no , b.quantity ,
 b.unit_price unit_price_aft_promo, b.amount gross_sales,
 case when a.sales_channel in ('CHAN-WAYFAIR', 'CHAN-WMDSV') then b.quantity  * b.unit_price end net_sales,
 b.discount,
 a.belong_to_company, name
from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily a
left join y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily b on a.external_doc_no = b.document_no
where UPPER(a.document_type) = 'ORDER'
 and to_char(posting_date,'YYYY-MM') = {posting_date}
 and a.bill_to_customer !~ 'INTC'
 AND a.internal_sales_channel in ('ASC FBA','WF CG','AVC DI')
and a.is_processed = 0 -- chi su dung khi day data len ggs cho AC check, van can day vao final de ghi nhan (nhung khong day len BC)
order by sales_channel , order_date , external_doc_no
'''

sql = QueryPostgre(sql= script , host=host, passwd=passwd, name=name, db=db)

# Connect to Google Sheets
json_path = 'json_path'
spreadsheet_key = '' # Input the spreadsheets_id
sheet_name = '' # Input the spreadsheets_name


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

si_to_gg = Gsheet_working(spreadsheet_key, sheet_name, json_file = json_path)
si_to_gg.Update_dataframe(sql, row_num = 1, col_num = 1, clear_sheet=True, copy_head = True )
