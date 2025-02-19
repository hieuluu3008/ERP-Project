-- DROP PROCEDURE y4a_erp.sp_prod_sales_invoice_02_2_asc_daily();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_prod_sales_invoice_02_2_asc_daily()
LANGUAGE plpgsql
AS $procedure$
	begin
--Daily load cho snapshot log
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log
--ASC
(with asc_order as(
	select
		distinct amazon_order_id ,
		sellersku,
		asin
	from y4a_cdm.y4a_dwa_amz_asc_sel_ord_dtl
	union all
	select
		distinct amazon_order_id ,
		sellersku,
		asin
	from y4a_cdm.prs_dwa_amz_asc_sel_ord_dtl),
asc_tbl as(
select
	date_time,
	c.customer_posting_group,
	Order_id,
	a.country,
	'14D' payment_term,
	c.belong_to_company,
	fulfillment,
	case
	when currency_code = 'USD' then null
	else currency_code
end currency_code,
	'CHAN-ASC' sales_channel,
	pims.pims_sku sku,
	case
		when fulfillment = 'FBM' then round((fbm_product_sales / quantity)::numeric, 2)
	when fulfillment = 'FBA' then round((fba_product_sales / quantity)::numeric, 2)
end unit_price,
	-(promotional_rebates) discount,
	quantity,
	round(case
			when fulfillment = 'FBM' then fbm_product_sales
			when fulfillment = 'FBA' then fba_product_sales
		  end, 2) amount,
	Order_id po_number,
	case
		when fulfillment = 'FBM' then 'ASC FBM'
		else 'ASC FBA'
	end internal_sales_channel,
	ord.asin as asin,
	comp.company_id y4a_company_id,
	a.company original_y4a_company_id
from
	y4a_analyst.y4a_amz_asc_pmt_dtl a
left join y4a_erp.y4a_erp_dim_y4a_company_id comp on
	comp.company_code = a.company
left join asc_order ord on a.sku = ord.sellersku and a.order_id = ord.amazon_order_id
left join y4a_finance.dim_pims_platform_product_profile_by_country pims on
	ord.asin = pims.platform_prd_id
	and pims.country = a.country
left join ( SELECT DISTINCT y4a_erp_prod_master_data_item.item_id,
           y4a_erp_prod_master_data_item.original_y4a_company_id,
           y4a_erp_prod_master_data_item.belong_to_company,customer_posting_group
          FROM y4a_erp.y4a_erp_prod_master_data_item) c on
		pims.pims_sku= c.item_id
	and c.original_y4a_company_id = a.company
where
	case
		when a.country = 'ESP' then date_time::date between '2024-01-01' and now()::date
		else date_time::date between '2024-01-01' and now()::date
	end
	--Báo cáo payment tháng 6 của thị trường ESP có rớt thêm 1 payment thanh toán ngày 31-05 -> Phải lấy thêm 31-05 để book tháng 6
	and type = 'Order'
	)
select
	date_time::date Order_date,
	null::date posting_date,
	date_time::date document_date,
	'Order' document_type,
	'131US-MER0001-AMZ' bill_to_customer,
	'131US-MER0001-AMZ' sell_to_customer,
	'PLF-AMZ-1' platform,
	customer_posting_group,
	null posting_description,
	payment_term,
	case
		when belong_to_company in ('Y4A', 'Hoang Thong', 'JTO') then concat(Order_id, '_', to_char(date_time::date, 'YYYYMMDD'))
	else concat(concat(Order_id, '_', to_char(date_time::date, 'YYYYMMDD')), '-', belong_to_company)
end external_doc_no,
case
	when upper(belong_to_company) in ('HMD', 'MOONKEY') then null
	when upper(belong_to_company) in ('Y4A', 'HOANG THONG', 'JTO') then
			case
		when fulfillment = 'FBM' then 'Unidentified'
		when fulfillment = 'FBA' then
					case
						when country = 'USA' then '157-FA-USA'
						when country = 'MEX' then '157-FA-MXN'
						when country = 'CAN' then '157-FA-CAN'
						when country = 'ARE' then '157-FA-UAE'
						when country = 'GBR' then '157-FA-UK'
						when country = 'JPN' then '157-FA-JPN'
						when country = 'ESP' then '157-FA-FRA'
						when country = 'FRA' then '157-FA-FRA'
						when country = 'DEU' then '157-FA-FRA'
						when country = 'ITA' then '157-FA-FRA'
						when country = 'SWE' then '157-FA-FRA'
						when country = 'NLD' then '157-FA-FRA'
						when country = 'POL' then '157-FA-FRA'
						when country = 'TUR' then '157-FA-FRA'
						when country = 'BEG' then '157-FA-BEG'
						else 'Unidentified'
					end
	end
	when upper(belong_to_company) is null then 'Unidentified'
	else '156-OBH-CA'
end
	as wh_location,
	currency_code currency,
	'DATAMART_SI' source_code,
	sales_channel,
	case
		when country = 'USA' then 'REG-USA'
		when country = 'MEX' then 'REG-MXN'
		when country = 'CAN' then 'REG-CAN'
		when country = 'ARE' then 'REG-UAE'
		when country = 'GBR' then 'REG-GBR'
		when country = 'JPN' then 'REG-JPN'
		when country = 'ESP' then 'REG-ESP'
		when country = 'FRA' then 'REG-FRA'
		when country = 'DEU' then 'REG-DEU'
		when country = 'ITA' then 'REG-ITA'
		when country = 'SWE' then 'REG-SWE'
		when country = 'NLD' then 'REG-NED'
		when country = 'POL' then 'REG-POL'
		when country = 'TUR' then 'REG-TUR'
		when country = 'BEG' then 'REG-BEG'
	end country,
	row_number() over(partition by case
		when belong_to_company in ('Y4A', 'Hoang Thong','JTO') then concat(Order_id, '_', to_char(date_time::date, 'YYYYMMDD'))
	else concat(concat(Order_id, '_', to_char(date_time::date, 'YYYYMMDD')), '-', belong_to_company)
end)* 10000 line_no,
	'ITEM' "type",
	left(sku, 4) "no",
	null description,
	'pcs' uom,
	unit_price,
	null line_discount_pct,
	discount,
	quantity,
	'VAT-NOT' vat_product_posting_group,
	amount,
	null::numeric vat_amount,
	null batch_seq,
	po_number,
	true is_valid_record,
	now() data_updated_time,
	internal_sales_channel,
	Order_id original_external_doc_no,
	asin,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company
from
	asc_tbl
)
;
--------------------------------------------------Xử lý giao dịch của Y4A, Hoang Thong-------------------------------------
--Daily load cho bảng temp
truncate y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp;
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp
	(Order_date ,
	posting_date ,
	document_date ,
	document_type ,
	bill_to_customer ,
	sell_to_customer ,
	platform ,
	customer_posting_group,
	posting_description ,
	payment_term ,
	external_doc_no,
	wh_location,
	currency ,
	source_code ,
	sales_channel,
	country ,
	line_no ,
	"type" ,
	"no",
	description,
	uom,
	unit_price,
	line_discount_pct,
	discount,
	quantity,
	vat_product_posting_group,
	amount,
	vat_amount ,
	batch_seq ,
	po_number ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no ,
	asin,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company)
with excluded_invoice as(
	select
		distinct b.external_doc_no
	from
		y4a_erp.y4a_erp_prod_sales_invoice_header_incremental b
	where
		UPPER(b.document_type) = 'ORDER' and sales_channel = 'CHAN-ASC'
union
	select
		distinct external_doc_no
	from
		y4a_erp.y4a_erp_prod_excluded_sales_invoice_header
)
select
	a.Order_date ,
--	now()::date posting_date ,
	CASE
       WHEN EXTRACT(MONTH FROM a.Order_date::date) < EXTRACT(MONTH FROM current_date::date) THEN date_trunc('month', now())::date
       WHEN EXTRACT(MONTH FROM a.Order_date::date) = EXTRACT(MONTH FROM current_date::date) THEN a.Order_date::date
       ELSE null
   END AS posting_date,
	a.document_date ,
	a.document_type ,
	a.bill_to_customer ,
	a.sell_to_customer ,
	a.platform ,
	a.customer_posting_group,
	concat(split_part(a.country,'-',2) ,'-',split_part(a.sales_channel,'-',2),': Booking revenue, Inv#',a.external_doc_no,'_',to_char( now()::date ,'YYMMDD')) posting_description ,
	a.payment_term ,
	a.external_doc_no,
	a.wh_location,
	a.currency ,
	a.source_code ,
	a.sales_channel,
	a.country ,
	a.line_no ,
	a."type" ,
	a."no",
	a.description,
	a.uom,
	a.unit_price,
	a.line_discount_pct,
	a.discount,
	a.quantity,
	a.vat_product_posting_group,
	a.amount,
	a.vat_amount ,
	a.batch_seq ,
	a.po_number ,
	a.is_valid_record ,
	a.data_updated_time,
	a.internal_sales_channel,
	a.original_external_doc_no ,
	a.asin,
	a.y4a_company_id,
	a.original_y4a_company_id,
	a.belong_to_company
from
	y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log a
	left join excluded_invoice b on a.external_doc_no  = b.external_doc_no
where
	a.data_updated_time = (
	select
		max(data_updated_time)
	from
		y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log)
		and a.is_valid_record = true
		and b.external_doc_no is null
		and a.belong_to_company in ('Y4A', 'Hoang Thong','JTO')
		and a.Order_date::date between '2024-01-01' and now()::date
		;
-- UPDATE LOCATION
	
	update y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp a
set wh_location = b.location
from (with txn as(
select
	distinct transaction_id ,
	case
		when warehouse in('ANA', 'BE-ANA', 'BRE') then '156-USA-CA'
		when warehouse = 'CRA' then '156-3PL-CB'
		when warehouse = 'FLO' then '156-3PL-FR'
		when warehouse = 'FON' then '156-3PL-FT'
		when warehouse = 'CQM' then '156-3PL-CA'
		else 'Unidentified'
	end location
from
		y4a_sop_ops.transaction_yes4all_detail
where
		"transaction" = 'ASC FBM'
	and log_date >= '2024-01-01'
	and ims_id is not null
	and qty <> 0
	and case when ims_id = 0 and log_id = 0 then false else true end
)select
	transaction_id ,
	count(distinct location) cnt,
	string_agg(distinct location,'') location
from
	txn
group by
	1
having count(distinct location) = 1) b
where a.original_external_doc_no = b.transaction_id and a.wh_location = 'Unidentified'
;
		
--Daily load cho sales invoice header
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily (
	Order_date,
	posting_date ,
	document_date,
	document_type,
	bill_to_customer ,
	sell_to_customer,
	platform,
	customer_posting_group,
	posting_description,
	payment_term_code,
	external_doc_no ,
	"location" ,
	currency ,
	source_code ,
	sales_channel ,
	country ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company,
	is_processed
)
select
	distinct
	Order_date,
	posting_date ,
	document_date,
	document_type,
	bill_to_customer ,
	sell_to_customer,
	platform,
	customer_posting_group,
	posting_description,
	payment_term,
	external_doc_no ,
	wh_location as "location" ,
	currency ,
	source_code ,
	sales_channel ,
	country ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company,
	 0 as is_processed
from
	y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp a
;
--Daily load cho sales invoice line
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily (
	document_no,
	line_no ,
	"type",
	"no" ,
	description ,
	uom ,
	unit_price ,
	line_discount_pct ,
	discount ,
	quantity,
	location_code,
	vat_product_posting_group ,
	amount ,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	original_asin_sku
)
	select
	distinct external_doc_no as document_no,
	line_no ,
	"type",
	"no" ,
	description ,
	uom ,
	round(unit_price::numeric,2) unit_price ,
	line_discount_pct ,
	discount ,
	quantity,
	wh_location location_code,
	vat_product_posting_group ,
	round(amount::numeric,2) amount,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	asin original_asin_sku
from
	y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp  a;
-----------------------------------------Xử lý giao dịch On-behalf - HMD & Moonkey--------------------------------
--Daily insert bảng on behalf
delete from y4a_erp.y4a_erp_prod_obh_sales_invoice_incremental;
insert into
	y4a_erp.y4a_erp_prod_obh_sales_invoice_incremental
	(Order_date ,
	posting_date ,
	document_date ,
	document_type ,
	bill_to_customer ,
	sell_to_customer ,
	platform ,
	customer_posting_group,
	posting_description ,
	payment_term ,
	external_doc_no,
	wh_location,
	currency ,
	source_code ,
	sales_channel,
	country ,
	line_no ,
	"type" ,
	"no",
	description,
	uom,
	unit_price,
	line_discount_pct,
	discount,
	quantity,
	vat_product_posting_group,
	amount,
	vat_amount ,
	batch_seq ,
	po_number ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no ,
	asin,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company)
select
	a.Order_date ,
	now()::date posting_date ,
	a.document_date ,
	a.document_type ,
	a.bill_to_customer ,
	a.sell_to_customer ,
	a.platform ,
	a.customer_posting_group,
	concat(split_part(a.country,'-',2) ,'-',split_part(a.sales_channel,'-',2),': Booking revenue, Inv#',a.external_doc_no,'_',to_char(now()::date,'YYMMDD')) posting_description ,
	a.payment_term ,
	a.external_doc_no,
	case when a.belong_to_company = 'HMD' then null else '156-OBH-CA' end as wh_location,
	a.currency ,
	a.source_code ,
	a.sales_channel,
	a.country ,
	a.line_no ,
	a."type" ,
	a."no",
	a.description,
	a.uom,
	a.unit_price,
	a.line_discount_pct,
	a.discount,
	a.quantity,
	a.vat_product_posting_group,
	a.amount,
	a.vat_amount ,
	a.batch_seq ,
	a.po_number ,
	a.is_valid_record ,
	a.data_updated_time,
	a.internal_sales_channel,
	a.original_external_doc_no ,
	a.asin,
	a.y4a_company_id,
	a.original_y4a_company_id,
	a.belong_to_company
from
	y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log a
	left join y4a_erp.y4a_erp_prod_sales_invoice_header_incremental b on a.external_doc_no = b.external_doc_no
where
	a.data_updated_time = (
	select
		max(data_updated_time)
	from
		y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log)
		and a.is_valid_record = true
		and b.external_doc_no is null
		and (a.belong_to_company not in ('Y4A', 'Hoang Thong','JTO') and a.belong_to_company is not null)
		and a.Order_date::date between '2024-01-01' and now()::date
		;
--Insert giao dịch On-behalf vào header
insert into y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily (
	Order_date,
	posting_date ,
	document_date,
	document_type,
	bill_to_customer ,
	sell_to_customer,
	platform,
	customer_posting_group,
	posting_description,
	payment_term_code,
	external_doc_no ,
	"location" ,
	currency ,
	source_code ,
	sales_channel ,
	country ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company,
	"name"
)select distinct
	Order_date,
	posting_date ,
	document_date,
	document_type,
	bill_to_customer ,
	sell_to_customer,
	platform,
	customer_posting_group,
	posting_description,
	payment_term,
	external_doc_no ,
	wh_location ,
	currency ,
	source_code ,
	sales_channel ,
	country ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company,
	bill_to_customer "name"
	from y4a_erp.y4a_erp_prod_obh_sales_invoice_incremental;
--Daily load cho sales invoice line OBH
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily (
	document_no,
	line_no ,
	"type",
	"no" ,
	description ,
	uom ,
	unit_price ,
	line_discount_pct ,
	discount ,
	quantity,
	location_code,
	vat_product_posting_group ,
	amount ,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	original_asin_sku
)select
	distinct external_doc_no as document_no,
	line_no ,
	'G/L Account' "type",
	'33890101' "no" ,
	description ,
	null uom ,
	round(unit_price::numeric,2) unit_price ,
	line_discount_pct ,
	discount ,
	1 quantity,
	wh_location location_code,
	vat_product_posting_group ,
	round(amount::numeric,2) amount,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	asin original_asin_sku
from
	y4a_erp.y4a_erp_prod_obh_sales_invoice_incremental;
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily (
	document_no,
	line_no ,
	"type",
	"no" ,
	description ,
	uom ,
	unit_price ,
	line_discount_pct ,
	discount ,
	quantity,
	location_code,
	vat_product_posting_group ,
	amount ,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	original_asin_sku
)select
	distinct external_doc_no as document_no,
	line_no ,
	'ITEM' "type",
	"no" ,
	description ,
	'pcs' as uom ,
	0 unit_price ,
	line_discount_pct ,
	0 discount ,
	quantity,
	wh_location location_code,
	vat_product_posting_group ,
	0 as amount,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	asin original_asin_sku
from
	y4a_erp.y4a_erp_prod_obh_sales_invoice_incremental
where belong_to_company not in ('HMD');
update y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily set uom = null where "no" in ('33890101','71190101','52120101');
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set posting_date = (select max(to_char(posting_date,'YYYY-MM-DD'))::date from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily)
--where to_char(posting_date,'YYYY-MM') != (select max(to_char(posting_date,'YYYY-MM')) from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily);
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set is_processed = 0 where is_processed is null ;
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set customer_posting_group = '1311-MER';
--update name
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set name = '331US-MER0019-NGU' where belong_to_company = 'General Union';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-MER0002-BLS' where belong_to_company = 'Bluestars';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-SER0002-BTT' where belong_to_company = 'Bitis';
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set name = 'US-SER0006-VTX' where belong_to_company = 'Vitox';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = '331US-SER0019-A&D' where belong_to_company = 'A&D';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-SER0003-HUR' where belong_to_company = 'HMD';
	END;
$procedure$
;

