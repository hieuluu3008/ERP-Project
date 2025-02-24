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

# Run procedure to ingest data
procedure_schema = 'y4a_erp'

procedure_name_7 = 'sp_y4a_erp_sel_exp_cop_inv_api_upt'
procedure_name_8 = 'sp_y4a_erp_sel_exp_ads_dtl_api_upt'
procedure_name_9 = 'sp_y4a_erp_sel_exp_inv_api_log'
procedure_name_10 = 'sp_y4a_erp_sel_exp_inv_api_incr'


call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_7)
call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_8)
call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_9)
call_stored_procedure(host, name, passwd, db,  procedure_schema, procedure_name_10)


