-- y4a_erp.view_y4a_erp_sales_invoice_reconcile_wm_wfs_daily source
CREATE OR REPLACE VIEW y4a_erp.view_y4a_erp_sales_invoice_reconcile_wm_wfs_daily
AS WITH wm_wfs AS (
        SELECT y4a_wfs_pmt_dtl.settlement_date AS order_date,
           NULL::date AS posting_date,
           y4a_wfs_pmt_dtl.settlement_date AS document_date,
           'Order'::text AS document_type,
           '131US-MER0002-WM'::text AS sell_to_customer,
           '131US-MER0002-WM'::text AS bill_to_customer,
           'PLF-WM-1'::text AS platform,
           '1311-MER'::text AS customer_posting_group,
           NULL::text AS posting_description,
           '14D'::text AS payment_term,
           concat(y4a_wfs_pmt_dtl.order_id, '_', to_char(y4a_wfs_pmt_dtl.report_date::timestamp with time zone, 'YYYYMMDD'::text)) AS external_doc_no,
           '156-USA-CA'::text AS wh_location,
           NULL::text AS currency,
           'DATAMART_SI'::text AS source_code,
           'CHAN-WMMKP'::text AS sales_channel,
           'REG-USA'::text AS country,
           row_number() OVER (PARTITION BY (concat(y4a_wfs_pmt_dtl.order_id, '_', to_char(y4a_wfs_pmt_dtl.report_date::timestamp with time zone, 'YYYYMMDD'::text)))) * 10000 AS line_no,
           'ITEM'::text AS type_,
           "left"(y4a_wfs_pmt_dtl.sku::text, 4) AS no,
           NULL::text AS description,
           'pcs'::text AS uom,
           round(((COALESCE(
               CASE
                   WHEN y4a_wfs_pmt_dtl.original_item_price = 0::double precision THEN NULL::double precision
                   ELSE y4a_wfs_pmt_dtl.original_item_price
               END, y4a_wfs_pmt_dtl.product_price) + y4a_wfs_pmt_dtl.shipping) / 1::double precision)::numeric, 2) AS unit_price,
           NULL::numeric AS line_discount_pct,
           NULL::numeric AS discount,
           1 AS quantity,
           'VATNOT'::text AS vat_product_posting_group,
           round((COALESCE(
               CASE
                   WHEN y4a_wfs_pmt_dtl.original_item_price = 0::double precision THEN NULL::double precision
                   ELSE y4a_wfs_pmt_dtl.original_item_price
               END, y4a_wfs_pmt_dtl.product_price) + y4a_wfs_pmt_dtl.shipping)::numeric, 2) AS amount,
           NULL::numeric AS vat_amount,
           NULL::text AS batch_seq,
           y4a_wfs_pmt_dtl.po_id AS po_number,
           true AS is_valid_record,
           now() AS data_updated_time,
           'WM WFS'::text AS internal_sales_channel,
           y4a_wfs_pmt_dtl.order_id AS original_external_doc_no,
           y4a_wfs_pmt_dtl.sku AS asin,
           comp.company_id AS y4a_company_id,
           'Y4ALLC'::text AS original_y4a_company_id,
           'Y4A'::text AS belong_to_company
          FROM y4a_analyst.y4a_wfs_pmt_dtl
            LEFT JOIN y4a_erp.y4a_erp_dim_y4a_company_id comp ON comp.company_code = 'Y4ALLC'::text
         WHERE y4a_wfs_pmt_dtl.transaction_type::text = 'SALE'::text AND y4a_wfs_pmt_dtl.settlement_date >= '2023-06-01'::date AND y4a_wfs_pmt_dtl.settlement_date <= now()::date
       ), final_wm_wfs AS (
        SELECT wm_wfs.external_doc_no AS rawdata_external_doc_no,
           sum(wm_wfs.amount) AS rawdata_amount
          FROM wm_wfs
         WHERE to_char(wm_wfs.order_date::timestamp with time zone, 'YYYY-MM'::text) = to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM'::text)
         GROUP BY wm_wfs.external_doc_no
       ), bc_wm_wfs AS (
        SELECT a_1.external_doc_no,
           sum(b_1.amount) AS bc_amount
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily a_1
            LEFT JOIN y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily b_1 ON a_1.external_doc_no = b_1.document_no
         WHERE a_1.internal_sales_channel = 'WM WFS'::text
         GROUP BY a_1.external_doc_no
       )
SELECT a.rawdata_external_doc_no,
   a.rawdata_amount,
   b.external_doc_no,
   b.bc_amount
  FROM final_wm_wfs a
    FULL JOIN bc_wm_wfs b ON a.rawdata_external_doc_no = b.external_doc_no
 WHERE a.rawdata_amount <> b.bc_amount OR b.external_doc_no IS NULL AND NOT (a.rawdata_external_doc_no IN ( SELECT DISTINCT y4a_erp_prod_sales_invoice_header_incremental.original_external_doc_no
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental));

