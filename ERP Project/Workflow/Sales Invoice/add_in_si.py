import os
import re
import sys
import numpy as np
import pandas as pd
from datetime import datetime
import pygsheets
import psycopg2
import json
import hashlib
import time
import logging
from psycopg2 import sql
from sqlalchemy import create_engine, types, Boolean
abs_path = os.path.dirname(__file__)
main_cwd = re.sub('Y4A_BA_Team.*','Y4A_BA_Team',abs_path)
os.chdir(main_cwd)
sys.path.append(main_cwd)
from Shared_Lib import pcconfig




pcconfig = pcconfig.Init()




host = '172.30.105.111'
name = 'y4a_dd_hieult'
passwd = 'JV!basdkj12312l'
db = 'y4a_datamart'
json_file = pcconfig['json_path']




sheet_id = '10JF72EJThBK1MCm0TGq4AfUupu3A_ItMhLexJIxX-rs'
sheet_name =  'Adjustment'


# Read data from ggsheet:
def Read_ggs_data(sheet_id, sheet_name, json_file,num_cols):
    # Authorize and open the Google Sheet
    gc = pygsheets.authorize(service_file=json_file)
    sh = gc.open_by_key(sheet_id)
   
    # Access the worksheet
    worksheet = sh.worksheet_by_title(sheet_name)
   
    # Get all values in one API call
    all_values = worksheet.get_all_values()
   
    # Extract column 2 directly from all_values and filter non-empty values
    col2 = [row[1] for row in all_values[1:]]  # Extract column 2 (index 1)
    filter_col2 = [i for i in col2 if i is not None and i != '']  # Remove empty values
   
    # Calculate data range
    data_range = len(filter_col2) + 1  # +1 to include the header row
   
    # Extract headers and data with slicing
    headers = all_values[0][:num_cols]  # First 33 columns of headers
    data = [row[:num_cols] for row in all_values[1:data_range]]  # First 33 columns of data rows
   
    # Create DataFrame and replace empty values with NaN
    df = pd.DataFrame(data, columns=headers).replace("", np.nan)


    # Clean column names
    df.columns = df.columns.str.lower().str.replace(r' \ ', '_').str.replace(r' / ', '_').str.replace(' ', '_').str.replace('\n', '_')
   
    return df


si_adhoc = Read_ggs_data(sheet_id, sheet_name, json_file, 33)


# Read data from PostgreSQL:


def QueryPostgre(sql, host, passwd, name, db):
    logging.info("Start db connection")
    db_connection = create_engine(f'postgresql://{name}:{passwd}@{host}:5432/{db}')
    with db_connection.connect() as con, con.begin():
        # df.to_sql('temp_amz_ads_profile_info', con,  schema='y4a_analyst',if_exists='append', index=False)
        df_ = pd.read_sql(sql, con)
        logging.info("Finish querying the data")
    return df_


cdh_si = QueryPostgre('SELECT * FROM y4a_erp.y4a_erp_prod_sales_invoice_incremental',host,passwd,name,db)


si_incremental = si_adhoc.loc[~si_adhoc['external_doc_no'].isin(cdh_si['external_doc_no']),:]


if si_incremental.empty:
    print('No new record to ingest')
    sys.exit()


si_incremental['source_code'] = 'DATAMART_SI'
si_incremental['batch_seq'] = np.nan
si_incremental['is_valid_record'] = True
si_incremental['is_exported'] = False
si_incremental['exporting_time'] = np.nan
si_incremental['error_type'] = np.nan
si_incremental['data_updated_time'] = datetime.now()
si_incremental['is_processed'] = 0


si_incremental['document_no'] = si_incremental['external_doc_no']
si_incremental['description'] = np.nan
si_incremental['line_discount_pct'] = np.nan
si_incremental['location_code'] = si_incremental['location']
si_incremental['discount'] = si_incremental['discount'] .apply(lambda x: np.nan if x == '' else x)
si_incremental['vat_amount'] = si_incremental['discount'] .apply(lambda x: np.nan if x == '' else x)


# Delete cdh table
def delete_data_from_table(table_name, condition):
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            database=db,
            user=name,
            password=passwd
        )
        # Create a cursor
        cursor = connection.cursor()
        # Build the DELETE query
        delete_query = f"DELETE FROM {table_name} WHERE {condition};"
        # Execute the DELETE query
        cursor.execute(delete_query)
        # Commit the transaction
        connection.commit()
        print("Data deleted successfully.")
    except (Exception, psycopg2.Error) as error:
        print("Error while deleting data:", error)
    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
            print("Database connection closed.")


delete_data_from_table(table_name='y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily',condition='1=1')
delete_data_from_table(table_name='y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily',condition='1=1')


# Prepare SI Header and SI Line
si_header = si_incremental.loc[:, ['order_date','posting_date','document_date','document_type','bill_to_customer','sell_to_customer','platform','customer_posting_group','posting_description','payment_term_code','external_doc_no','location','currency','source_code','sales_channel','country','batch_seq','is_valid_record','data_updated_time','is_exported','exporting_time','internal_sales_channel','original_external_doc_no','is_processed','error_type','y4a_company_id','original_y4a_company_id','belong_to_company','name'
]].drop_duplicates()


si_line = si_incremental.loc[:,['document_no','line_no','type','no','description','uom','unit_price','line_discount_pct','discount','quantity','location_code','vat_product_posting_group','amount','vat_amount','country','sales_channel','platform','batch_seq','po_number','data_updated_time','is_exported','exporting_time','original_external_doc_no','is_processed','original_asin_sku'
]]


# Ingest CDH
def df_to_postgres_cdh(df, db_table_name, schema, if_exists='append', **kwargs):
    db_connection = create_engine(f'postgresql://{name}:{passwd}@{host}:5432/{db}')
    with db_connection.connect() as con, con.begin():
        df.to_sql(name = db_table_name, con=con, schema=schema,if_exists=if_exists, index=False)
        print('Successfully ingest ',df.shape[0],' records in',db_table_name)


#Ingest header
df_to_postgres_cdh(si_header, db_table_name = 'y4a_erp_prod_sales_invoice_header_incremental_daily',schema='y4a_erp')
#Ingest line
df_to_postgres_cdh(si_line, db_table_name = 'y4a_erp_prod_sales_invoice_line_incremental_daily',schema='y4a_erp')