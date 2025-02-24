-- STEP 4.4: Get invoice with location = ' Unidentified'
with a as(
with final as (
with erp as (
		with a as (
			select * from y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily where
			document_no in (select distinct external_doc_no
							from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily where 1=1 and "location" = 'Unidentified')
				)
	select b.original_external_doc_no
	, c.po_id
	, a."no" as sku
	, location_code
	, a.country
	, b.internal_sales_channel
	from a
	left join y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily b on a.document_no= b.external_doc_no
	left join y4a_analyst.y4a_wfs_pmt_dtl c on b.original_external_doc_no = c.order_id
)
, trans as (
	select
	 "transaction"
	, transaction_id
	, warehouse
	, sku
	, ims_id
	from y4a_sop_ops.transaction_yes4all_detail
	where 1=1
	)
select original_external_doc_no invoice_number,po_id,erp.sku,location_code, "transaction", warehouse, ims_id, country, internal_sales_channel
from erp
left join trans on (case when po_id is null then original_external_doc_no else po_id end) = transaction_id
)
select invoice_number
, string_agg(distinct po_id::text,'|') po_id
, string_agg(distinct sku,',') sku
, string_agg(distinct location_code,',') "location"
, string_agg(distinct country,',') country
, string_agg(distinct internal_sales_channel,',') sales_channel
, string_agg(distinct warehouse,',') warehouse
, string_agg(distinct ims_id::text,'|') ims_id
from final
where 1=1
group by 1
order by 6,1
) select invoice_number,sku,"location",case when sales_channel = 'WM WFS' then 'WM MKP' else sales_channel end sales_channel,warehouse, ims_id from a
WHERE 1=1
order by sales_channel
;


--- Check Outbound vs SI WF
WITH erp_si AS (
select
a.order_date,
a.external_doc_no , a.location ,
a.original_external_doc_no,
a.original_y4a_company_id, a.belong_to_company, a.country,
b."no" sku, concat(a.original_external_doc_no,b."no") AS si_key
,sum(b.quantity) quantity,
sum(b.amount) gross_sales,
sum(case when a.sales_channel in ('CHAN-WAYFAIR', 'CHAN-WMDSV') then b.quantity  * b.unit_price END) net_sales
from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental a
left join y4a_erp.y4a_erp_prod_sales_invoice_line_incremental b on a.external_doc_no = b.document_no
where UPPER(a.document_type) = 'ORDER'
and to_char(posting_date,'YYYY-MM') = '2025-01'
and a.bill_to_customer !~ 'INTC'
AND internal_sales_channel IN ('WF')
AND external_doc_no = 'CS565223591'
GROUP BY 1,2,3,4,5,6,7,8,9
),
temptb AS (
select
concat(itm.sale_order_number,itm.sku) AS ob_key,
itm.sale_order_number,
itm.sku,
sum(itm.actual_exported_qty) quantity,
max(isn.issue_date::date),
string_agg(DISTINCT isn.issue_date::date::text,',') outbound_date,
count(DISTINCT isn.issue_date::date) cnt_date,
string_agg(DISTINCT isn.issue_code,',') issue_code,
count(DISTINCT isn.issue_code) cnt_ob,
string_agg(DISTINCT dwh.erp_warehouse ,',') erp_warehouse,
count(DISTINCT dwh.erp_warehouse) cnt_wh,
max(isn.issue_date::date) ob_date
from y4a_erp.y4a_ims_issue_note isn
inner join y4a_erp.y4a_ims_issue_item itm on isn.id = itm.issue_note_id
inner join y4a_erp.y4a_ims_warehouse wh on isn.warehouse_from_id = wh.id
INNER JOIN y4a_dim.dim_warehouse dwh ON dwh.warehouse = wh.short_name
where 1=1
AND isn.status = 'COMPLETED'
 AND isn.issue_date::date >= '2024-10-26'
 AND isn.issue_date::date <= '2024-12-31'
 and issue_type = 'SALES_ORDERS'
GROUP BY 1,2,3
ORDER BY itm.sku
)
, finaltb AS (
SELECT
b.si_key,
b. order_date, b.external_doc_no, b."location", b.country, 'WF' internal_sales_channel,
b.original_external_doc_no,
b.sku, b.quantity,
a.sale_order_number, NULL shipment_id ,a.sku ob_sku, a.quantity ob_qty,a.issue_code, a.erp_warehouse
, a.outbound_date
,b.original_y4a_company_id, b.gross_sales, b.net_sales
FROM temptb a
RIGHT JOIN erp_si b ON b.si_key = a.ob_key
-- RIGHT JOIN erp_si b ON b.original_external_doc_no = a.sale_order_number
WHERE 1=1
-- AND cnt_date > 1
-- AND a.quantity != b.quantity
-- AND a.sale_order_number IS NOT NULL
-- AND b.order_date < a.ob_date
-- AND b.external_doc_no !~ '-OBH-'
ORDER BY 1 DESC, 2
)
SELECT * FROM finaltb;
SELECT * FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
WHERE internal_sales_channel = 'WF'
AND "location" != 'Unidentified'
AND original_external_doc_no NOT IN (SELECT DISTINCT original_external_doc_no FROM finaltb)
;
--- Check Outbound vs SI ASC FBM
WITH erp_si AS (
select
a.order_date,
a.external_doc_no , a.location ,
a.original_external_doc_no,
a.original_y4a_company_id, a.belong_to_company, a.country,
b."no" sku, concat(a.original_external_doc_no,b."no") AS si_key
,sum(b.quantity) quantity,
sum(b.amount) gross_sales,
sum(case when a.sales_channel in ('CHAN-WAYFAIR', 'CHAN-WMDSV') then b.quantity  * b.unit_price END) net_sales
from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily a
left join y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily b on a.external_doc_no = b.document_no
where UPPER(a.document_type) = 'ORDER'
and to_char(posting_date,'YYYY-MM') = '2025-01'
and a.bill_to_customer !~ 'INTC'
AND internal_sales_channel IN ('ASC FBM')
-- AND "location" = 'Unidentified'
GROUP BY 1,2,3,4,5,6,7,8,9
),
temptb AS (
select
--concat(itm.sale_order_number,itm.sku) AS ob_key,
itm.sale_order_number,
string_agg(itm.sku,',') sku,
sum(itm.actual_exported_qty) quantity,
max(isn.issue_date::date),
string_agg(DISTINCT isn.issue_date::date::text,',') outbound_date,
count(DISTINCT isn.issue_date::date) cnt_date,
string_agg(DISTINCT isn.issue_code,',') issue_code,
count(DISTINCT isn.issue_code) cnt_ob,
string_agg(DISTINCT dwh.erp_warehouse ,',') erp_warehouse,
count(DISTINCT dwh.erp_warehouse) cnt_wh,
max(isn.issue_date::date) ob_date
from y4a_erp.y4a_ims_issue_note isn
inner join y4a_erp.y4a_ims_issue_item itm on isn.id = itm.issue_note_id
inner join y4a_erp.y4a_ims_warehouse wh on isn.warehouse_from_id = wh.id
INNER JOIN y4a_dim.dim_warehouse dwh ON dwh.warehouse = wh.short_name
where 1=1
AND isn.status = 'COMPLETED'
 and isn.created_date >= '2024-04-01'
 and isn.created_date < '2025-01-26'
--  AND isn.issue_date::date >= '2024-10-26'
--  AND isn.issue_date::date <= '2024-12-31'
 and issue_type = 'SALES_ORDERS'
--  AND sale_order_number IN ('114-6211690-1342639')
GROUP BY 1
ORDER BY 1
)
, finaltb AS (
SELECT
b. order_date, b.external_doc_no, b."location", b.country, 'ASC FBM' internal_sales_channel,
b.original_external_doc_no,
b.sku, b.quantity,
a.sale_order_number, NULL shipment_id ,a.sku ob_sku, a.quantity ob_qty,a.issue_code, a.erp_warehouse
, a.outbound_date
-- , a.ob_date
,b.original_y4a_company_id, b.gross_sales, b.net_sales
-- count(*)
FROM temptb a
-- RIGHT JOIN erp_si b ON b.si_key = a.ob_key
RIGHT JOIN erp_si b ON b.original_external_doc_no = a.sale_order_number
WHERE 1=1
-- AND cnt_date > 1
-- AND a.quantity != b.quantity AND a.sku = b.sku
-- AND a.sale_order_number IS NULL
AND b.order_date < a.ob_date
AND b.external_doc_no !~ '-OBH-'
ORDER BY 9 DESC
)
SELECT * FROM finaltb;

