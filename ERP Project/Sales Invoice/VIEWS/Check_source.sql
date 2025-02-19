-- y4a_erp.view_prod_sales_invoice_data_source_tracking source
CREATE OR REPLACE VIEW y4a_erp.view_prod_sales_invoice_data_source_tracking
AS SELECT 'WF Invoice'::text AS data_source,
   'y4a_cdm.y4a_dwb_wyf_fin_inv_dsv_upt'::text AS tbl_name,
   max(y4a_dwb_wyf_fin_inv_dsv_upt.run_time) AS latest_run_time,
   max(y4a_dwb_wyf_fin_inv_dsv_upt.invoice_date) AS latest_report_date,
   'Wayfair'::text AS platform
  FROM y4a_cdm.y4a_dwb_wyf_fin_inv_dsv_upt
UNION ALL
SELECT 'WF Dropship Invoice'::text AS data_source,
   'y4a_cdm.y4a_dwb_wyf_dso'::text AS tbl_name,
   max(y4a_dwb_wyf_dso.run_date) AS latest_run_time,
   max(y4a_dwb_wyf_dso.invoice_date) AS latest_report_date,
   'Wayfair'::text AS platform
  FROM y4a_cdm.y4a_dwb_wyf_dso
UNION ALL
SELECT 'WF Order'::text AS data_source,
   'y4a_cdm.y4a_dwb_wyf_ord_exp_upt'::text AS tbl_name,
   max(y4a_dwb_wyf_ord_exp_upt.run_time) AS latest_run_time,
   max(y4a_dwb_wyf_ord_exp_upt.po_date) AS latest_report_date,
   'Wayfair'::text AS platform
  FROM y4a_cdm.y4a_dwb_wyf_ord_exp_upt
UNION ALL
SELECT 'WF Home Pricing'::text AS data_source,
   'y4a_cdm.y4a_dwb_wyf_prd_prc_hom_backup'::text AS tbl_name,
   max(y4a_dwb_wyf_prd_prc_hom_backup.run_time) AS latest_run_time,
   NULL::timestamp without time zone AS latest_report_date,
   'Wayfair'::text AS platform
  FROM y4a_cdm.y4a_dwb_wyf_prd_prc_hom_backup
UNION ALL
SELECT 'ASC Idzo Order'::text AS data_source,
   'y4a_cdm.y4a_dwa_amz_asc_sel_ord_dtl'::text AS tbl_name,
   max(y4a_dwa_amz_asc_sel_ord_dtl.run_date) AS latest_run_time,
   NULL::timestamp without time zone AS latest_report_date,
   'Amazon'::text AS platform
  FROM y4a_cdm.y4a_dwa_amz_asc_sel_ord_dtl
UNION ALL
SELECT 'ASC PREP Order'::text AS data_source,
   'y4a_cdm.prs_dwa_amz_asc_sel_ord_dtl'::text AS tbl_name,
   max(prs_dwa_amz_asc_sel_ord_dtl.run_date) AS latest_run_time,
   NULL::timestamp without time zone AS latest_report_date,
   'Amazon'::text AS platform
  FROM y4a_cdm.prs_dwa_amz_asc_sel_ord_dtl
UNION ALL
SELECT 'ASC Payment'::text AS data_source,
   'y4a_analyst.y4a_amz_asc_pmt_dtl'::text AS tbl_name,
   max(y4a_amz_asc_pmt_dtl.run_time) AS latest_run_time,
   max(y4a_amz_asc_pmt_dtl.date_time) AS latest_report_date,
   'Amazon'::text AS platform
  FROM y4a_analyst.y4a_amz_asc_pmt_dtl
UNION ALL
SELECT 'ASC Payment PRS'::text AS data_source,
   'y4a_cdm.prs_dwb_amz_asc_pmt_rpt'::text AS tbl_name,
   max(prs_dwb_amz_asc_pmt_rpt.run_date) AS latest_run_time,
   max(prs_dwb_amz_asc_pmt_rpt.date_time) AS latest_report_date,
   'Amazon'::text AS platform
  FROM y4a_cdm.prs_dwb_amz_asc_pmt_rpt
UNION ALL
SELECT 'ASC Payment Idzo'::text AS data_source,
   'y4a_cdm.y4a_dwb_amz_asc_pmt_rpt'::text AS tbl_name,
   max(y4a_dwb_amz_asc_pmt_rpt.run_date) AS latest_run_time,
   max(y4a_dwb_amz_asc_pmt_rpt.date_time) AS latest_report_date,
   'Amazon'::text AS platform
  FROM y4a_cdm.y4a_dwb_amz_asc_pmt_rpt
UNION ALL
SELECT 'AVC Sum'::text AS data_source,
   'y4a_cdm.y4a_dwc_amz_avc_inv_sum'::text AS tbl_name,
   max(y4a_dwc_amz_avc_inv_sum.run_date) AS latest_run_time,
   max(y4a_dwc_amz_avc_inv_sum.invoice_date) AS latest_report_date,
   'Amazon'::text AS platform
  FROM y4a_cdm.y4a_dwc_amz_avc_inv_sum
UNION ALL
SELECT 'AVC Detail'::text AS data_source,
   'y4a_cdm.y4a_dwc_amz_avc_inv_dtl'::text AS tbl_name,
   max(y4a_dwc_amz_avc_inv_dtl.run_date) AS latest_run_time,
   NULL::timestamp without time zone AS latest_report_date,
   'Amazon'::text AS platform
  FROM y4a_cdm.y4a_dwc_amz_avc_inv_dtl
 GROUP BY 'AVC Detail'::text
UNION ALL
SELECT 'Shipment_3PL_Location'::text AS data_source,
   'y4a_cdm.y4a_dwc_amz_avc_shm_sum'::text AS tbl_name,
   max(y4a_dwc_amz_avc_shm_sum.run_date) AS latest_run_time,
   NULL::timestamp without time zone AS latest_report_date,
   'Amazon'::text AS platform
  FROM y4a_cdm.y4a_dwc_amz_avc_shm_sum
 GROUP BY 'Shipment_3PL_Location'::text;

