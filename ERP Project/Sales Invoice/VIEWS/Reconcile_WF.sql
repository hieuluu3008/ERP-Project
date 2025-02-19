-- y4a_erp.view_y4a_erp_sales_invoice_reconcile_wf_daily source
CREATE OR REPLACE VIEW y4a_erp.view_y4a_erp_sales_invoice_reconcile_wf_daily
AS WITH wf AS (
        WITH wf_gross_sale AS (
                SELECT y4a_erp_view_si_wayfair_base_cost.account_name,
                   y4a_erp_view_si_wayfair_base_cost.supplier_part_number,
                   y4a_erp_view_si_wayfair_base_cost.effective_from,
                   y4a_erp_view_si_wayfair_base_cost.effective_to,
                   y4a_erp_view_si_wayfair_base_cost.base_cost
                  FROM y4a_erp.y4a_erp_view_si_wayfair_base_cost
               )
        SELECT a_1.invoice_date::date AS order_date,
           NULL::date AS posting_date,
           a_1.invoice_date::date AS document_date,
           'Order'::text AS document_type,
           '131US-MER0001-WF'::text AS bill_to_customer,
           '131US-MER0001-WF'::text AS sell_to_customer,
           'PLF-WF-1'::text AS platform,
           '1311-MER'::text AS customer_posting_group,
           NULL::text AS posting_description,
           '60D'::text AS payment_term,
           a_1.invoice_num AS external_doc_no,
           '156-USA-CA'::text AS wh_location,
           NULL::text AS currency,
           'DATAMART_SI'::text AS source_code,
           'CHAN-WAYFAIR'::text AS sales_channel,
           'REG-USA'::text AS country,
           row_number() OVER (PARTITION BY a_1.invoice_num) * 10000 AS line_no,
           'ITEM'::text AS type,
           "left"(b_1.item_number::text, 4) AS no,
           ''::text AS description,
           'pcs'::text AS uom,
           b_1.wholesale_price AS unit_price,
           NULL::numeric AS line_discount_pct,
           NULL::numeric AS discount,
           b_1.quantity,
           'VATNOT'::text AS vat_product_posting_group,
           round(c.base_cost * b_1.quantity::numeric, 2) AS amount,
           NULL::numeric AS vat_amount,
           NULL::text AS batch_seq,
           a_1.po_num AS po_number,
           true AS is_valid_record,
           now() AS data_updated_time,
           'WF'::text AS internal_sales_channel,
           a_1.invoice_num AS original_external_doc_no,
           b_1.item_number AS asin,
           comp.company_id AS y4a_company_id,
           'PR-ENTERPRISE'::text AS original_y4a_company_id,
           'Y4A'::text AS belong_to_company
          FROM y4a_cdm.y4a_dwb_wyf_fin_inv_dsv_upt a_1
            LEFT JOIN y4a_cdm.y4a_dwb_wyf_ord_exp_upt b_1 ON a_1.po_num::text = b_1.po_number::text
            LEFT JOIN wf_gross_sale c ON b_1.item_number::text = c.supplier_part_number::text AND b_1.po_date_time >= c.effective_from AND b_1.po_date_time <= c.effective_to AND a_1.account_name = c.account_name
            LEFT JOIN y4a_erp.y4a_erp_dim_y4a_company_id comp ON comp.company_code = 'PR-ENTERPRISE'::text
         WHERE 1 = 1 AND a_1.invoice_date::date >= '2023-03-01'::date AND a_1.invoice_date::date <= now()::date AND "right"(a_1.invoice_num::text, 3) !~~ '%CM%'::text AND a_1.amount > 0::numeric AND b_1.order_status::text <> 'Cancelled'::text
       ), final_wf AS (
        SELECT wf.external_doc_no AS y4a_cdm_external_doc_no,
           sum(wf.amount) AS y4a_cdm_amount
          FROM wf
         WHERE to_char(wf.order_date::timestamp with time zone, 'YYYY-MM'::text) = to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM'::text)
         GROUP BY wf.external_doc_no
       ), bc_wf AS (
        SELECT a_1.original_external_doc_no,
           sum(b_1.amount) AS bc_amount
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily a_1
            LEFT JOIN y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily b_1 ON a_1.external_doc_no = b_1.document_no
         WHERE a_1.internal_sales_channel = ANY (ARRAY['WF'::text, 'WF CG'::text])
         GROUP BY a_1.original_external_doc_no
       )
SELECT a.y4a_cdm_external_doc_no,
   a.y4a_cdm_amount,
   b.original_external_doc_no,
   b.bc_amount
  FROM final_wf a
    FULL JOIN bc_wf b ON a.y4a_cdm_external_doc_no::text = b.original_external_doc_no
 WHERE a.y4a_cdm_amount <> b.bc_amount OR b.original_external_doc_no IS NULL AND NOT (a.y4a_cdm_external_doc_no::text IN ( SELECT DISTINCT y4a_erp_prod_sales_invoice_header_incremental.original_external_doc_no
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental));

