Ingest data to final tables (Suggest: download data before ingest to final table to save log)


-- Header Table:
--insert into y4a_erp.y4a_erp_prod_sales_invoice_header_incremental
(order_date, posting_date, document_date, document_type, bill_to_customer
           , sell_to_customer, platform, customer_posting_group, posting_description, payment_term_code
           , external_doc_no, location, source_code, sales_channel,currency, country, is_valid_record, data_updated_time, exporting_time, internal_sales_channel
           , original_external_doc_no, is_processed , y4a_company_id, original_y4a_company_id, belong_to_company,name
           )
          
select order_date, posting_date, document_date, document_type, bill_to_customer
           , sell_to_customer, platform, customer_posting_group, posting_description, payment_term_code
           , external_doc_no, location, source_code, sales_channel, currency, country, is_valid_record, data_updated_time, exporting_time, internal_sales_channel
           , original_external_doc_no, is_processed , y4a_company_id, original_y4a_company_id, belong_to_company, name
from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
where 1 = 1
and is_processed = 0
and external_doc_no not in (select distinct external_doc_no from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental)
;
-- Line Table:
--insert into y4a_erp.y4a_erp_prod_sales_invoice_line_incremental
(document_no, line_no, "type", "no", description , uom, unit_price
			, line_discount_pct, discount, quantity, location_code
			, vat_product_posting_group, amount, vat_amount, country, sales_channel
			, platform, po_number, data_updated_time, exporting_time, original_external_doc_no,original_asin_sku
			)
			
select distinct document_no
, line_no, "type", "no", description, uom, unit_price, line_discount_pct, discount, quantity, location_code
, vat_product_posting_group, amount, vat_amount, country, sales_channel, platform, po_number, data_updated_time, exporting_time, original_external_doc_no,original_asin_sku
from y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily
where 1=1
and document_no not in (select distinct document_no from y4a_erp.y4a_erp_prod_sales_invoice_line_incremental)
and document_no in (
select distinct external_doc_no from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
where 1=1
AND is_processed = 0
)
order by document_no desc
;