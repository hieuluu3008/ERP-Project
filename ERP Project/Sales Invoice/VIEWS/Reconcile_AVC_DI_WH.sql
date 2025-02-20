-- y4a_erp.view_y4a_erp_sales_invoice_reconcile_avc_di_wh_daily source
CREATE OR REPLACE VIEW y4a_erp.view_y4a_erp_sales_invoice_reconcile_avc_di_wh_daily
AS WITH vendor AS (
        SELECT y4a_dwc_amz_avc_pco_sum.poid AS po,
               CASE
                   WHEN y4a_dwc_amz_avc_pco_sum.vendor::text = ANY (ARRAY['YES4A'::character varying::text, 'YES4M'::character varying::text]) THEN 'AVC WH'::text
                   ELSE 'AVC DI'::text
               END AS sales_channel
          FROM y4a_cdm.y4a_dwc_amz_avc_pco_sum
         GROUP BY y4a_dwc_amz_avc_pco_sum.poid, (
               CASE
                   WHEN y4a_dwc_amz_avc_pco_sum.vendor::text = ANY (ARRAY['YES4A'::character varying::text, 'YES4M'::character varying::text]) THEN 'AVC WH'::text
                   ELSE 'AVC DI'::text
               END)
       ), agg_inv_dtl AS (
        SELECT y4a_dwc_amz_avc_inv_dtl.invoice_number,
           sum(y4a_dwc_amz_avc_inv_dtl.quantity) AS sum,
           sum(y4a_dwc_amz_avc_inv_dtl.received_quantity) AS received_quantity,
           sum(y4a_dwc_amz_avc_inv_dtl.total_amount) AS total_amount,
           sum(y4a_dwc_amz_avc_inv_dtl.received_amount) AS received_amount
          FROM y4a_cdm.y4a_dwc_amz_avc_inv_dtl
         GROUP BY y4a_dwc_amz_avc_inv_dtl.invoice_number
       ), inv_check AS (
        SELECT DISTINCT a_1.invoice_number
          FROM y4a_cdm.y4a_dwc_amz_avc_inv_sum a_1
            LEFT JOIN agg_inv_dtl b_1 ON a_1.invoice_number::text = b_1.invoice_number::text
         WHERE a_1.invoice_date::date >= '2023-06-01'::date AND a_1.invoice_date::date <= now()::date AND abs(a_1.invoice_amount - b_1.total_amount) > 2::numeric
       UNION
        SELECT DISTINCT amz_avc_deleted_inv_sum.invoice_number
          FROM y4a_analyst.amz_avc_deleted_inv_sum
       ), avc_tbl AS (
        SELECT a_1.invoice_date,
               CASE
                   WHEN c.sales_channel = 'AVC DI'::text THEN '120D'::text
                   ELSE '90D'::text
               END AS payment_term,
           a_1.invoice_number,
           a_1.country,
           COALESCE(pims.pims_sku, eu_case.sku) AS pims_sku,
           b_1.unit_cost,
           b_1.quantity,
           b_1.total_amount,
           b_1.po_number,
           c.sales_channel,
           b_1.asin,
           comp.company_id AS y4a_company_id,
           'Y4ALLC'::text AS original_y4a_company_id,
           item_master.belong_to_company,
           item_master.customer_posting_group
          FROM y4a_cdm.y4a_dwc_amz_avc_inv_sum a_1
            LEFT JOIN y4a_cdm.y4a_dwc_amz_avc_inv_dtl b_1 ON a_1.invoice_number::text = b_1.invoice_number::text
            LEFT JOIN vendor c ON b_1.po_number::text = c.po::text
            LEFT JOIN y4a_finance.dim_pims_platform_product_profile_by_country pims ON b_1.asin::text = pims.platform_prd_id AND a_1.country::text = pims.country
            LEFT JOIN ( SELECT DISTINCT tb_poms_com_inv_dri.amz_po,
                   tb_poms_com_inv_dri.asin,
                   tb_poms_com_inv_dri.sku
                  FROM y4a_analyst.tb_poms_com_inv_dri) eu_case ON eu_case.amz_po = b_1.po_number::text AND eu_case.asin = b_1.asin::text
            LEFT JOIN y4a_erp.y4a_erp_dim_y4a_company_id comp ON comp.company_code = 'Y4ALLC'::text
            LEFT JOIN y4a_erp.y4a_erp_prod_master_data_item item_master ON COALESCE(pims.pims_sku, eu_case.sku) = item_master.item_id::text AND item_master.original_y4a_company_id = 'Y4ALLC'::text
            LEFT JOIN inv_check ON a_1.invoice_number::text = inv_check.invoice_number::text
         WHERE inv_check.invoice_number IS NULL AND b_1.invoice_number::text !~~ '%SCR'::text AND b_1.invoice_number::text !~~ '%PCR'::text AND a_1.invoice_status::text !~~ '%Paid%'::text AND a_1.invoice_date::date >= '2023-06-01'::date AND a_1.invoice_date::date <= now()::date
       ), final_avc AS (
        SELECT avc_tbl.invoice_number AS y4a_cdm_external_doc_no,
           avc_tbl.invoice_date,
           sum(avc_tbl.total_amount) AS y4a_cdm_amount
          FROM avc_tbl
         WHERE to_char(avc_tbl.invoice_date, 'YYYY-MM'::text) = to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM'::text)
         GROUP BY avc_tbl.invoice_number, avc_tbl.invoice_date
       ), bc_avc AS (
        SELECT a_1.original_external_doc_no,
           sum(b_1.amount) AS bc_amount
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily a_1
            LEFT JOIN y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily b_1 ON a_1.external_doc_no = b_1.document_no
         WHERE a_1.internal_sales_channel = ANY (ARRAY['AVC DI'::text, 'AVC WH'::text, 'AVC GPE'::text])
         GROUP BY a_1.original_external_doc_no
       )
SELECT a.y4a_cdm_external_doc_no,
   a.y4a_cdm_amount,
   b.original_external_doc_no,
   b.bc_amount,
   a.invoice_date
  FROM final_avc a
    FULL JOIN bc_avc b ON a.y4a_cdm_external_doc_no::text = b.original_external_doc_no
 WHERE a.y4a_cdm_amount <> b.bc_amount OR b.original_external_doc_no IS NULL AND NOT (a.y4a_cdm_external_doc_no::text IN ( SELECT DISTINCT y4a_erp_prod_sales_invoice_header_incremental.original_external_doc_no
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental))
 ORDER BY a.invoice_date;

