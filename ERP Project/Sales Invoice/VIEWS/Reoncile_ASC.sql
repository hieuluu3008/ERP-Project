-- y4a_erp.view_y4a_erp_sales_invoice_reconcile_asc_daily source
CREATE OR REPLACE VIEW y4a_erp.view_y4a_erp_sales_invoice_reconcile_asc_daily
AS WITH asc_tbl AS (
        SELECT a_1.date_time,
           c.customer_posting_group,
           a_1.order_id,
           a_1.country,
           '14D'::text AS payment_term,
           c.belong_to_company,
           a_1.fulfillment,
               CASE
                   WHEN a_1.currency_code::text = 'USD'::text THEN NULL::character varying
                   ELSE a_1.currency_code
               END AS currency_code,
           'CHAN-ASC'::text AS sales_channel,
           a_1.sku,
               CASE
                   WHEN a_1.fulfillment = 'FBM'::text THEN round((a_1.fbm_product_sales + a_1.shipping_credits) / a_1.quantity, 2)
                   WHEN a_1.fulfillment = 'FBA'::text THEN round((a_1.fba_product_sales + a_1.shipping_credits) / a_1.quantity, 2)
                   ELSE NULL::numeric
               END AS unit_price,
           - a_1.promotional_rebates AS discount,
           a_1.quantity,
           round(
               CASE
                   WHEN a_1.fulfillment = 'FBM'::text THEN a_1.fbm_product_sales + a_1.shipping_credits
                   WHEN a_1.fulfillment = 'FBA'::text THEN a_1.fba_product_sales + a_1.shipping_credits
                   ELSE NULL::numeric
               END, 2) AS amount,
           a_1.order_id AS po_number,
               CASE
                   WHEN a_1.fulfillment = 'FBM'::text THEN 'ASC FBM'::text
                   ELSE 'ASC FBA'::text
               END AS internal_sales_channel,
           NULL::text AS asin,
           comp.company_id AS y4a_company_id,
           a_1.company AS original_y4a_company_id
          FROM y4a_analyst.y4a_amz_asc_pmt_dtl a_1
            LEFT JOIN y4a_erp.y4a_erp_dim_y4a_company_id comp ON comp.company_code = a_1.company::text
            LEFT JOIN y4a_erp.y4a_erp_prod_master_data_item c ON "left"(a_1.sku::text, 4) = c.item_id::text AND c.original_y4a_company_id = a_1.company::text
         WHERE
               CASE
                   WHEN a_1.country::text = 'ESP'::text THEN a_1.date_time >= '2023-05-31'::date AND a_1.date_time <= now()::date
                   ELSE a_1.date_time >= '2023-06-01'::date AND a_1.date_time <= now()::date
               END AND a_1.type::text = 'Order'::text
       ), final_asc_tbl AS (
        SELECT to_char(asc_tbl.date_time::timestamp with time zone, 'YYYY-MM'::text) AS raw_month,
           asc_tbl.order_id AS y4a_cdm_external_doc_no,
           sum(asc_tbl.amount) AS y4a_cdm_amount
          FROM asc_tbl
         GROUP BY (to_char(asc_tbl.date_time::timestamp with time zone, 'YYYY-MM'::text)), asc_tbl.order_id
       ), bc_asc AS (
        SELECT to_char(a_1.order_date::timestamp with time zone, 'YYYY-MM'::text) AS bc_month,
           a_1.original_external_doc_no,
           sum(b_1.amount) AS bc_amount
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily a_1
            LEFT JOIN y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily b_1 ON a_1.external_doc_no = b_1.document_no
         WHERE a_1.sales_channel = 'CHAN-ASC'::text
         GROUP BY (to_char(a_1.order_date::timestamp with time zone, 'YYYY-MM'::text)), a_1.original_external_doc_no
       )
SELECT a.raw_month,
   a.y4a_cdm_external_doc_no,
   a.y4a_cdm_amount,
   b.bc_month,
   b.original_external_doc_no,
   b.bc_amount
  FROM final_asc_tbl a
    FULL JOIN bc_asc b ON a.y4a_cdm_external_doc_no::text = b.original_external_doc_no AND a.raw_month = b.bc_month
 WHERE b.original_external_doc_no IS NULL OR a.y4a_cdm_amount <> b.bc_amount AND NOT (a.y4a_cdm_external_doc_no::text IN ( SELECT DISTINCT y4a_erp_prod_sales_invoice_header_incremental.original_external_doc_no
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental))
 ORDER BY a.raw_month DESC, b.bc_month DESC;

