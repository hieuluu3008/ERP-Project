-- y4a_erp.view_y4a_erp_sales_invoice_reconcile_avc_ds_daily source
CREATE OR REPLACE VIEW y4a_erp.view_y4a_erp_sales_invoice_reconcile_avc_ds_daily
AS WITH avc_ds AS (
        SELECT e.invoicedate,
           a_1.invoicenumber,
           'CHAN-AVC'::text AS sales_channel,
           '60D'::text AS payment_term,
           c.belong_to_company,
           pims.pims_sku,
           round(a_1.subtotal / a_1.quantity, 2) AS unit_price,
           a_1.quantity,
           round(a_1.subtotal, 2) AS amount,
           a_1.invoicenumber AS po_number,
           'AVC DS'::text AS internal_sales_channel,
           'USA'::text AS country,
           a_1.asin,
           comp.company_id AS y4a_company_id,
           'Y4ALLC'::text AS original_y4a_company_id
          FROM y4a_cdm.y4a_dwc_amz_avc_dsi_dtl_itm a_1
            LEFT JOIN y4a_cdm.y4a_dwc_amz_avc_dsi_dtl_sum e ON a_1.invoicenumber::text = e.invoicenumber::text
            LEFT JOIN y4a_finance.dim_pims_platform_product_profile_by_country pims ON a_1.asin::text = pims.platform_prd_id AND pims.country = e.country::text
            LEFT JOIN y4a_erp.y4a_erp_dim_y4a_company_id comp ON comp.company_code = 'Y4ALLC'::text
            LEFT JOIN ( SELECT DISTINCT y4a_erp_prod_master_data_item.item_id,
                   y4a_erp_prod_master_data_item.original_y4a_company_id,
                   y4a_erp_prod_master_data_item.belong_to_company,
                   y4a_erp_prod_master_data_item.customer_posting_group
                  FROM y4a_erp.y4a_erp_prod_master_data_item) c ON pims.pims_sku = c.item_id::text AND c.original_y4a_company_id = 'Y4ALLC'::text
         WHERE e.invoicedate::date >= '2024-06-01'::date AND e.invoicedate <= now() AND e.status::text <> 'REJECTED'::text
       ), final_avc_ds AS (
        SELECT avc_ds.invoicenumber AS y4a_cdm_external_doc_no,
           avc_ds.invoicedate,
           sum(avc_ds.amount) AS y4a_cdm_amount
          FROM avc_ds
         WHERE to_char(avc_ds.invoicedate, 'YYYY-MM'::text) = to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM'::text)
         GROUP BY avc_ds.invoicenumber, avc_ds.invoicedate
       ), bc_avc_ds AS (
        SELECT a_1.original_external_doc_no,
           sum(b_1.amount) AS bc_amount
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily a_1
            LEFT JOIN y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily b_1 ON a_1.external_doc_no = b_1.document_no
         WHERE a_1.internal_sales_channel = 'AVC DS'::text
         GROUP BY a_1.original_external_doc_no
       )
SELECT a.y4a_cdm_external_doc_no,
   a.y4a_cdm_amount,
   b.original_external_doc_no,
   b.bc_amount,
   NULL::date AS document_date,
   a.invoicedate
  FROM final_avc_ds a
    FULL JOIN bc_avc_ds b ON a.y4a_cdm_external_doc_no::text = b.original_external_doc_no
 WHERE a.y4a_cdm_amount <> b.bc_amount OR b.original_external_doc_no IS NULL AND NOT (a.y4a_cdm_external_doc_no::text IN ( SELECT DISTINCT y4a_erp_prod_sales_invoice_header_incremental.original_external_doc_no
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental))
 ORDER BY a.invoicedate;

