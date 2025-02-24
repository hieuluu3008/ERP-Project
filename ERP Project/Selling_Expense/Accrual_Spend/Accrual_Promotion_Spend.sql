-- DROP PROCEDURE y4a_erp.sp_prod_sel_exp_promotion_accrual(text);
CREATE OR REPLACE PROCEDURE y4a_erp.sp_prod_sel_exp_promotion_accrual(IN accounting_period text)
LANGUAGE plpgsql
AS $procedure$
	begin
/*
* Trình tự chạy flow
* (1) Lưu Promotion Invoice vào bảng log
* (2) Lưu Estimated Promotion vào bảng log
* (3) Reconcile (1) và (2) để tính Accrual Promotion và lưu vào bảng accrual
* (4) Ingest dữ liệu vào GL Journal Line
*/
--Bước 1: lưu snapshot của Invoice tại thời điểm chạy sổ ERP
insert into y4a_erp.y4a_erp_sel_exp_promotion_inv_log
with pro_dtl as(
select
	invoice_number ,
	a.invoice_date ,
	order_date report_date,
	coalesce(a.promotion_id, c.promotion_id) promotion_id ,
	asin,
	a.country ,
	cost_currency ,
	quantity ,
	promotion_cost
from
	y4a_cdm.y4a_dwc_amz_cop_pro a
	left join y4a_cdm.y4a_dwc_amz_cop_ovr b on a.invoice_number = b.invoice_id
	LEFT JOIN y4a_cdm.y4a_dwb_amz_pro_inf_mst  c ON c.funding_agreement  = b.agreement_id
where
	UPPER(a.funding_type) !~ 'GUARANTEED'
order by
	invoice_date desc) ,
prom_ovr as(
select
	invoice_id invoice_number,
	invoice_date ,
	case
		when (upper(funding_type :: text) ~ 'STRAIGHT PAYMENT' :: text
			and upper(agreement_title :: text) ~ 'MERCHANDISING') then split_part(agreement_title, ':', 2)
	end promotion_id,
	null asin,
	country,
	currency cost_currency,
	original_balance,
	case
		when (upper(funding_type :: text) ~ 'STRAIGHT PAYMENT' :: text
			and upper(agreement_title :: text) ~ 'MERCHANDISING') then 'Base Merchandising Fee'
		when (upper(funding_type :: text) ~ 'VENDOR' :: text
			and upper(agreement_title :: text) !~ 'VPC') then 'Promotions'
		when (upper(funding_type :: text) ~ 'VENDOR' :: text
			and upper(agreement_title :: text) ~ 'VPC') then 'Coupon'
	end promo_type
from
	y4a_cdm.y4a_dwc_amz_cop_ovr
where
	1 = 1
	--Base Merchandising fee
	and (upper(funding_type :: text) ~ 'STRAIGHT PAYMENT' :: text
		and upper(agreement_title :: text) ~ 'MERCHANDISING')
	--Promotion
		or(upper(funding_type :: text) ~ 'VENDOR' :: text
			and upper(agreement_title :: text) !~ 'VPC')
		--Coupon
			or(upper(funding_type :: text) ~ 'VENDOR' :: text
				and upper(agreement_title :: text) ~ 'VPC'))
select
	coalesce(a.invoice_number, b.invoice_number) invoice_number,
	coalesce(a.invoice_date, b.invoice_date) invoice_date,
	report_date,
	case when coalesce (a.promotion_id,
	b.promotion_id) is null then 'VM Promotion' else coalesce (a.promotion_id,
	b.promotion_id) end  promotion_id,
	a.asin,
	coalesce(a.country, b.country) country,
	coalesce(a.cost_currency, a.cost_currency) cost_currency,
	a.quantity,
	--Bao gồm Promo cost (Không gồm clip fee) và Base Merchandising Fee
	coalesce(promotion_cost, original_balance) promotion_cost,
	case
		when b.promo_type = 'Base Merchandising Fee' then 0
		else ((b.original_balance - sum(promotion_cost) over(partition by a.invoice_number))/ sum(a.quantity) over(partition by a.invoice_number))* quantity
	end clp_fee,
	b.original_balance invoice_amount,
	promo_type,
	now() data_updated_time
from
	pro_dtl a
full join prom_ovr b on
	a.invoice_number = b.invoice_number;
--Bước 2: Lưu snapshot của dữ liệu estimated promo tại thời điểm chạy sổ ERP
insert into y4a_erp.tb_y4a_erp_sel_exp_acr_pro_cpn_logs_v1
select
	report_date ,
	promotion_id ,
	invoices_list::text ,
	country ,
	platform ,
	sale_channel ,
	platform_prd_id ,
	pims_sku ,
	local_currency ,
	accrual_amt_usd ,
	pro_est_adj_usd ,
	invoiced_amt_usd ,
	accrual_amt_lc ,
	pro_est_adj_lc ,
	invoiced_amt_lc ,
	note ,
	now() run_time,
	now()::date posting_date
from
	y4a_finance.fact_y4a_agg_pro_spent_by_sku
where platform = 'AMZ' and report_date >= '2023-01-01'
;
--Bước 3: Chạy ra số Accrual và lưu lại detail
insert into y4a_erp.tb_pnl_sellin_acr_pro_cpn
with final as(
(
--2023: AC yêu cầu không chạy lại số estimation mà chỉ update thêm invoice. do đó 2023 lấy lại số Estimation được log lại đợt tháng 04/2024
with est_pro_23 as(
select
	report_date ,
	coupon_id ,
	country ,
	platform ,
	sale_channel ,
	platform_prd_id ,
	pro_est_adj_usd,
	pro_est_adj_lcy
from
	y4a_erp.tb_y4a_erp_sel_exp_acr_pro_cpn_logs_v1
where
	to_char(run_time::date, 'YYYY-MM') = '2024-04'
	and report_date between '2023-06-01' and '2023-12-31'
)	
,
inv_pro_23 as(
select
	report_date ,
	promotion_id ,
	country ,
	platform ,
	sale_channel ,
	invoices_list list_invoice_number,
	platform_prd_id,
	invoiced_amt_usd,
	invoiced_amt_lc invoiced_amt_lcy
from
	y4a_finance.fact_y4a_agg_pro_spent_by_sku
where
	report_date between '2023-06-01' and '2023-12-31'),
final as(
select
	a.*,
	b.list_invoice_number::text ,
	b.invoiced_amt_usd,
	b.invoiced_amt_lcy
from
	est_pro_23 a
left join inv_pro_23 b on
	coalesce(a.coupon_id,'') = coalesce(b.promotion_id,'')
	and a.country = b.country
	and a.platform = b.platform
	and a.sale_channel = b.sale_channel
	and a.platform_prd_id = b.platform_prd_id
	and a.report_date = b.report_date
)select
	to_char(report_date, 'YYYY-MM') report_month ,
	concat(accounting_period,'-25')::date posting_date ,
--	concat(accounting_period,'-31')::date posting_date , -- only Dec
	coupon_id ,
	platform_prd_id ,
	a.platform ,
	country ,
	string_agg(distinct list_invoice_number,'|')list_invoice_number,
	sum(pro_est_adj_usd) est_amt_usd,
	sum(pro_est_adj_lcy) est_amt_lcy,
	sum(invoiced_amt_usd) invoiced_amt_usd,
	sum(invoiced_amt_lcy) invoiced_amt_lcy,
	case
		when sum(pro_est_adj_usd) > coalesce(sum(invoiced_amt_usd), 0) then sum(pro_est_adj_usd) - coalesce(sum(invoiced_amt_usd), 0)
		else 0
	end accrual_amt_usd,
	case
		when sum(pro_est_adj_lcy) > coalesce(sum(invoiced_amt_lcy), 0) then sum(pro_est_adj_lcy) - coalesce(sum(invoiced_amt_lcy), 0)
		else 0
	end accrual_amt_lcy
from
	final a
left join y4a_analyst.dim_pims_platform_product_profile b on a.platform_prd_id = b.platform_product_id and b.platform_short = 'AMZ'
where
	coalesce(b.pims_company,'Y4A') IN ('Y4A', 'Hoang Thong','General Union','JTO')
group by
	1,
	2,
	3,
	4,
	5,
	6
order by
	1)
union
--2024: Vẫn re-estimate hằng tháng
(select
	to_char(report_date, 'YYYY-MM') report_month ,
	concat(accounting_period,'-25')::date posting_date ,
--	concat(accounting_period,'-31')::date posting_date , -- only Dec
	coupon_id ,
	platform_prd_id ,
	a.platform ,
	country ,
	string_agg(distinct list_invoice_number,'|')list_invoice_number,
	sum(pro_est_adj_usd) est_amt_usd,
	sum(pro_est_adj_lcy) est_amt_lcy,
	sum(invoiced_amt_usd) invoiced_amt_usd,
	sum(invoiced_amt_lcy) invoiced_amt_lcy,
	case
		when sum(pro_est_adj_usd) > coalesce(sum(invoiced_amt_usd), 0) then sum(pro_est_adj_usd) - coalesce(sum(invoiced_amt_usd), 0)
		else 0
	end accrual_amt_usd,
	case
		when sum(pro_est_adj_lcy) > coalesce(sum(invoiced_amt_lcy), 0) then sum(pro_est_adj_lcy) - coalesce(sum(invoiced_amt_lcy), 0)
		else 0
	end accrual_amt_lcy
from
	y4a_erp.tb_y4a_erp_sel_exp_acr_pro_cpn_logs_v1 a
left join y4a_analyst.dim_pims_platform_product_profile b on a.platform_prd_id = b.platform_product_id and b.platform_short = 'AMZ'
where
	a.run_time = (
	select
		max(run_time)
	from
		y4a_erp.tb_y4a_erp_sel_exp_acr_pro_cpn_logs_v1)
	and report_date between '2024-01-01'
		and concat(accounting_period,'-25')::date
--		and concat(accounting_period,'-31')::date -- only Dec
	and coalesce(b.pims_company,'Y4A') IN ('Y4A', 'Hoang Thong','General Union','JTO')
group by
	1,
	2,
	3,
	4,
	5,
	6
order by
	1))
select
	posting_date,
	report_month,
	coupon_id,
	list_invoice_number,
	platform,
	country,
	platform_prd_id,
	est_amt_usd ,
	est_amt_lcy,
	invoiced_amt_usd,
	invoiced_amt_lcy,
	accrual_amt_usd,
	accrual_amt_lcy,
	now() run_time,
	concat('PROMOACRAMZ',country,to_char(posting_date,'YYYYMM')) external_doc_no
from
	final a;
--Bước 4: Ingest revert accrual vào bảng GL
insert into y4a_erp.y4a_erp_prod_selling_expense_incremental
with revert_accrual as(
select
	posting_date ,
	external_doc_no ,
	platform ,
	country,
	round(sum(accrual_amt_usd)::numeric,2) accrual_amt_usd
from
	y4a_erp.tb_pnl_sellin_acr_pro_cpn
where
	to_char(posting_date, 'YYYY-MM') = to_char(concat(accounting_period,'-01')::date - interval '1 MONTH','YYYY-MM')
group by
	1,
	2,
	3,
	4
having
	sum(accrual_amt_usd) != 0
)select
	concat(accounting_period,'-25')::date posting_date,
--	concat(accounting_period,'-31')::date posting_date, -- only Dec
	''due_date,
	''document_type,
	''document_no,
	''bal_vat_bus_posting_group,
	''vat_prod_posting_group,
	'G/L Account'account_type,
	'33510102'account_no,
	'Accrued expenses_Short-term_Operation expenses'account_name,
	'' posting_group,
	concat('AVC ', case when country = 'ARE' then 'UAE' when country = 'MEX' then 'MXN' else country end,': Booking revert accrual Promotion expenses at ', to_char(now()::date-INTERVAL'5 days'-INTERVAL'1 month','MonYYYY') ) descriptions,
	''gen_prod_posting_group,
	accrual_amt_usd  amount,
	'G/L Account'bal_account_type,
	'64160102'bal_account_no,
	''bal_vat_prod_posting_group,
	''bal_gen_prod_posting_group,
	''currency_code,
	'CHAN-AVC'channel_code,
	concat('REG-',case when country = 'ARE' then 'UAE' when country = 'MEX' then 'MXN' else country end) region_code ,
	'PLF-AMZ-1'platform_code,
	external_doc_no external_document_no,
	''company_ic_code,
	now() data_updated_time,
	external_doc_no original_invoice_company,
	null::date document_date,
	null exporting_time,
	2 is_processed,
	null error_type,
	'f4acc907-580b-ee11-8f6e-000d3aa09c10'y4a_company_id,
	'b0f0b2b9-0a61-ee11-8df1-00224858547d' journal_id,
	'Y4ALLC'original_y4a_company_id,
	''accounts,
	'DATA-USA'batch_name,
	concat('PROM-AVC ',case when country = 'ARE' then 'UAE' when country = 'MEX' then 'MXN' else country end,'_AMZ') other_name
from
	revert_accrual;
--Bước 5: Ingest accrual vào bảng GL
insert into y4a_erp.y4a_erp_prod_selling_expense_incremental
with accrual as(
select
	posting_date ,
	external_doc_no ,
	platform ,
	country,
	round(sum(accrual_amt_usd)::numeric,2) accrual_amt_usd
from
	y4a_erp.tb_pnl_sellin_acr_pro_cpn
where
	to_char(posting_date, 'YYYY-MM') = accounting_period
	AND run_time =(SELECT max(run_time) FROM y4a_erp.tb_pnl_sellin_acr_pro_cpn)
group by
	1,
	2,
	3,
	4
having
	sum(accrual_amt_usd) != 0
)select
	posting_date,
	''due_date,
	''document_type,
	''document_no,
	''bal_vat_bus_posting_group,
	''vat_prod_posting_group,
	'G/L Account'account_type,
	'64160102'account_no,
	'Selling exp_Promotion'account_name,
	'' posting_group,
	concat('AVC ', case when country = 'ARE' then 'UAE'  when country = 'MEX' then 'MXN' else country end,': Booking accrual Promotion expenses at ', to_char(posting_date ,'MonYYYY') ) descriptions,
	''gen_prod_posting_group,
	accrual_amt_usd  amount,
	'G/L Account'bal_account_type,
	'33510102'bal_account_no,
	''bal_vat_prod_posting_group,
	''bal_gen_prod_posting_group,
	''currency_code,
	'CHAN-AVC'channel_code,
	concat('REG-',case when country = 'ARE' then 'UAE'  when country = 'MEX' then 'MXN' else country end) region_code ,
	'PLF-AMZ-1'platform_code,
	external_doc_no external_document_no,
	''company_ic_code,
	now() data_updated_time,
	external_doc_no original_invoice_company,
	null::date document_date,
	null exporting_time,
	2 is_processed,
	null error_type,
	'f4acc907-580b-ee11-8f6e-000d3aa09c10'y4a_company_id,
	'b0f0b2b9-0a61-ee11-8df1-00224858547d' journal_id,
	'Y4ALLC'original_y4a_company_id,
	''accounts,
	'DATA-USA'batch_name,
	concat('PROM-AVC ',case when country = 'ARE' then 'UAE'  when country = 'MEX' then 'MXN' else country end,'_AMZ') other_name
from
	accrual;
	END;
$procedure$
;

