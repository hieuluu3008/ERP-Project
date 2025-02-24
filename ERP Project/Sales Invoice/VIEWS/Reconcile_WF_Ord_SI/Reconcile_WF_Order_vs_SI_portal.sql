-- y4a_erp.y4a_erp_view_si_wayfair_reconcile_si_net_sales_vs_invoice_porta source
CREATE OR REPLACE VIEW y4a_erp.y4a_erp_view_si_wayfair_reconcile_si_net_sales_vs_invoice_porta
AS SELECT a.invoice_num,
   a.invoice_date,
   a.sku_list,
   a.invoice_amount,
   a.po_amount,
   a.si_amount,
   a.si_amount_testing,
   a.si_net_sales
  FROM y4a_erp.y4a_erp_view_si_wayfair_reconcile_vs_po_vs_invoice_daily a
 WHERE 1 = 1 AND a.invoice_amount <> a.si_net_sales AND NOT (a.invoice_num::text IN ( SELECT DISTINCT view_y4a_erp_sales_invoice_suffix_reconcile_wf_daily.document_no
          FROM y4a_erp.view_y4a_erp_sales_invoice_suffix_reconcile_wf_daily)) AND NOT (a.invoice_num::text IN ( SELECT DISTINCT y4a_erp_prod_sales_invoice_header_incremental.external_doc_no
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental))
 ORDER BY (abs(a.invoice_amount - a.po_amount)) DESC;