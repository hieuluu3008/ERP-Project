--- Chỉ giữ lại những invoices không vượt qua cut-off time trong kỳ:
DELETE FROM y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily
WHERE document_no IN (  SELECT DISTINCT external_doc_no
						FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
						WHERE document_date >= {posting_date});
				
DELETE FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
WHERE document_date >= {posting_date};
