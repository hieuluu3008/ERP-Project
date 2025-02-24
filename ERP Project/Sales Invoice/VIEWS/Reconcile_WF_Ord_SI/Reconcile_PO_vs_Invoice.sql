-- y4a_erp.y4a_erp_view_si_wayfair_reconcile_vs_po_vs_invoice_daily source
CREATE OR REPLACE VIEW y4a_erp.y4a_erp_view_si_wayfair_reconcile_vs_po_vs_invoice_daily
AS WITH wf_gross_sale AS (
        SELECT y4a_erp_view_si_wayfair_base_cost.account_name,
           y4a_erp_view_si_wayfair_base_cost.supplier_part_number,
           y4a_erp_view_si_wayfair_base_cost.effective_from,
           y4a_erp_view_si_wayfair_base_cost.effective_to,
           y4a_erp_view_si_wayfair_base_cost.base_cost
          FROM y4a_erp.y4a_erp_view_si_wayfair_base_cost
       ), reconcile_invoice_po AS (
        SELECT a.invoice_num,
           a.invoice_date,
           b.sku_list,
           sum(a.amount) AS invoice_amount,
           b.po_amount,
           c.si_amount,
           b.si_amount_testing,
           c.si_net_sales
          FROM y4a_cdm.y4a_dwb_wyf_fin_inv_dsv_upt a
            LEFT JOIN ( SELECT DISTINCT a_1.po_number,
                   sum(a_1.wholesale_price * a_1.quantity::numeric) AS po_amount,
                   string_agg(a_1.item_number::text, ' | '::text) AS sku_list,
                   sum(b_1.base_cost * a_1.quantity::numeric) AS si_amount_testing
                  FROM y4a_cdm.y4a_dwb_wyf_ord_exp_upt a_1
                    LEFT JOIN wf_gross_sale b_1 ON a_1.item_number::text = b_1.supplier_part_number::text AND a_1.po_date_time >= b_1.effective_from AND a_1.po_date_time <= b_1.effective_to AND a_1.account_name::text = b_1.account_name
                 GROUP BY a_1.po_number) b ON a.invoice_num::text = b.po_number::text
            LEFT JOIN ( SELECT DISTINCT y4a_erp_prod_sales_invoice_line_incremental_daily.document_no,
                   sum(y4a_erp_prod_sales_invoice_line_incremental_daily.amount) AS si_amount,
                   sum(y4a_erp_prod_sales_invoice_line_incremental_daily.unit_price * y4a_erp_prod_sales_invoice_line_incremental_daily.quantity) AS si_net_sales
                  FROM y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily
                 GROUP BY y4a_erp_prod_sales_invoice_line_incremental_daily.document_no) c ON a.invoice_num::text = c.document_no
         GROUP BY a.invoice_num, a.invoice_date, b.po_amount, c.si_amount, b.sku_list, b.si_amount_testing, c.si_net_sales
       )
SELECT reconcile_invoice_po.invoice_num,
   reconcile_invoice_po.invoice_date,
   reconcile_invoice_po.sku_list,
   reconcile_invoice_po.invoice_amount,
   COALESCE(reconcile_invoice_po.po_amount, 0::numeric) AS po_amount,
   reconcile_invoice_po.si_amount,
   reconcile_invoice_po.si_amount_testing,
   reconcile_invoice_po.si_net_sales
  FROM reconcile_invoice_po
 WHERE 1 = 1 AND abs(reconcile_invoice_po.invoice_amount - COALESCE(reconcile_invoice_po.po_amount, 0::numeric)) >= 0.01 AND reconcile_invoice_po.invoice_date::date >= '2023-08-01'::date
 ORDER BY reconcile_invoice_po.invoice_date;
