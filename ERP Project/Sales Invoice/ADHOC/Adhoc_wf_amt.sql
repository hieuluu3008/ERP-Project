-- Update giá những invoice có sku chênh 0.01$
with temp as (
select document_no, "no" as sku, unit_price, quantity, (unit_price * quantity) amt, b.amount as invoice_amount, (b.amount - (unit_price * quantity))/quantity diff from y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily a
left join y4a_cdm.y4a_dwb_wyf_fin_inv_dsv_upt b on document_no = invoice_num
left join y4a_erp.y4a_erp_view_si_wayfair_reconcile_si_net_sales_vs_invoice_porta c on a.po_number = c.invoice_num and a."no" = left(c.sku_list,4)
where length(c.sku_list) < 6
and (b.amount - (unit_price * quantity))/quantity = 0.01
order by po_number, line_no desc
)
UPDATE y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily as line_tbl
set unit_price = temp.unit_price + temp.diff
from temp
where temp.document_no = line_tbl.document_no
and temp.sku = line_tbl."no"
and temp.diff = 0.01;
