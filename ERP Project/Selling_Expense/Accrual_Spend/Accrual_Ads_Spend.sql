-- DROP PROCEDURE y4a_erp.sp_prod_sel_exp_ads_accrual(text);
CREATE OR REPLACE PROCEDURE y4a_erp.sp_prod_sel_exp_ads_accrual(IN accounting_period text)
LANGUAGE plpgsql
AS $procedure$
	begin
/*
* Accrual Ads bao gồm: AMZ, WM DSV và Google Ads
* Trình tự các bước:
* ====================================AMZ====================================
* (1) Break Ads Invoice của AMZ theo level SKU-ASIN
* (2) Lưu log của bước 1
* (3) Reconcile giữa AMZ Ads Spend với AMZ Ads Invoice để tính số Accrual và lưu vào bảng
* (4) Từ bước 3, ingest dữ liệu vào bảng GL Journal Line
* ===================================WM DSV==================================
* (1) Lưu Ads spend của WM DSV vào bảng log
* (2) ingest dữ liệu vào bảng GL Journal Line
* ===================================GG Ads=================================
* (1) Lưu Ads spend của GG Ads vào bảng log
* (2) ingest dữ liệu vào bảng GL Journal Line
*/
--==============================================================================AMAZON====================================================================================================
--Bước 1: Break Ads Invoice theo SKU dựa vào Ads spend
call y4a_analyst.prc_tb_amz_ads_inv_by_asin_allocated();
--Bước 2: lưu snapshot của Invoice tại thời điểm chạy sổ ERP
insert into y4a_erp.y4a_erp_sel_exp_ads_inv_log
select
	summary_id,
	summary_invoicedate ,
	summary_fromdate ,
	summary_todate ,
	country ,
	currency ,
	campaignid ,
	inv_amt ,
	cmp_allocated_inv_amt ,
	report_date ,
	platform_item_id ,
	cmp_spend_amt ,
	asin_spend_amt ,
	asin_spend_pct ,
	asin_allocated_inv_amt ,
	now() run_time
from
	y4a_analyst.tb_amz_ads_inv_dtl_by_asin_allocated_v01
where to_char(summary_invoicedate,'YYYY') = '2025';
--Bước 3: Matching Invoice vs. Spend để tính số Accrual
--Lưu ý: Riêng với Google Ads và DSP (tài khoản Bme-US), Accrual = Spend từ ngày 26 của tháng M-1 đến 25 của tháng M (Lý do cách tính Accrual của 2 loại Ads này khác với các loại khác:
-- (1) CDH chưa cào Invoice của 2 loại ads này về; (2) Platform thường issue 1 invoice/tháng và charge full nguyên tháng
insert into y4a_erp.tb_pnl_sellin_amz_acr_ads
with ads_spend_dtl as (
(
--Không lấy spend của GG Ads và DSP(Account Bme-US) do cách tính accrual khác
--Không lấy affiliate vì chưa matching được giữa spend và invoice (key campaignid giữa spend và invoice đang khác nhau )
select
	report_date ,
	a.platform ,
	country_code ,
	accname ,
	campaignid ,
	case
		when UPPER(platform_item_id) ~ 'UNALLOCATED' then 'unallocated_asin'
		else platform_item_id
	end platform_item_id,
	sum(spend) ads_spend,
	sum(spend)*c.to_usd  ads_spend_usd
from
	y4a_analyst.y4a_agg_ads_perf_by_sku a
left join y4a_analyst.dim_country b on a.country_code = b.country_code_3 
left join y4a_analyst.dim_currency_exchange_rate c on b.major_currency_code = c.currency_code and a.report_date = c.run_date
left join y4a_analyst.dim_pims_platform_product_profile d on a.platform_item_id = d.platform_product_id and a.platform = d.platform_short
where
	to_char(report_date,'YYYY') = '2025'
	and report_date <= concat(accounting_period, '-25')::date
--	and report_date <= concat(accounting_period, '-31')::date -- only in Dec
	and a.platform not in ('WF', 'WM')
	and (d.pims_company in ('Y4A','Hoang Thong', 'General Union','JTO') or d.pims_company is null) and accname != 'GG Ads' and accname != 'Bme - US' and advertisingtype != 'Affiliate'
group by
	1,
	2,
	3,
	4,
	5,
	6,
	c.to_usd)
--union
--(
----Lấy Account Bme-US nhưng chỉ lấy range từ 26 tháng T-1 đến 25 tháng T
--select
--	report_date ,
--	a.platform ,
--	country_code ,
--	accname ,
--	campaignid ,
--	case
--		when UPPER(platform_item_id) ~ 'UNALLOCATED' then 'unallocated_asin'
--		else platform_item_id
--	end platform_item_id,
--	sum(spend) ads_spend,
--	sum(spend)*c.to_usd  ads_spend_usd
--from
--	y4a_analyst.y4a_agg_ads_perf_by_sku a
--left join y4a_analyst.dim_country b on a.country_code = b.country_code_3 
--left join y4a_analyst.dim_currency_exchange_rate c on b.major_currency_code = c.currency_code and a.report_date = c.run_date
--left join y4a_analyst.dim_pims_platform_product_profile d on a.platform_item_id = d.platform_product_id and a.platform = d.platform_short
--where
--	to_char(report_date,'YYYY') = '2024' and report_date between concat(to_char(concat(accounting_period, '-25')::date - interval '1 MONTH','YYYY-MM'),'-26')::date  and concat(accounting_period, '-25')::date  --filter 26 cua tháng M-1 tới 25 tháng M
--	and a.platform not in ('WF', 'WM') and  accname = 'Bme - US'
--group by
--	1,
--	2,
--	3,
--	4,
--	5,
--	6,
--	c.to_usd)
),
ads_spend_agg as(
--1 campaignid có >1 campaignname
select
	platform ,
	country_code ,
	accname ,
	campaignid ,
	platform_item_id,
	sum(ads_spend) ads_spend,
	sum(ads_spend_usd) ads_spend_usd
from
	ads_spend_dtl
group by
	1,
	2,
	3,
	4,
	5
),
ads_inv_agg as(
select
	'AMZ' platform,
	campaignid,
	null account_name,
	country,
	currency,
	case
		when upper(platform_item_id) ~ 'UNALLOCATED' then 'unallocated_asin'
		else platform_item_id
	end platform_item_id ,
	string_agg(distinct summary_id, ' ; ') list_invoice_number,
	sum(asin_allocated_inv_amt) invoiced_amount,
	sum(asin_allocated_inv_amt* b.to_usd)  invoiced_amount_usd
from
	y4a_erp.y4a_erp_sel_exp_ads_inv_log a
	left join y4a_analyst.dim_currency_exchange_rate b on a.currency = b.currency_code and a.summary_invoicedate = b.run_date
	left join y4a_analyst.dim_pims_platform_product_profile c on a.platform_item_id = c.platform_product_id and c.platform_short = 'AMZ'
where
	summary_invoicedate between '2024-01-01'
		and concat(accounting_period, '-25')::date
--		and concat(accounting_period, '-31')::date -- only in Dec
	and to_char(report_date ,'YYYY') = '2025'
	and a.run_time = (select max(run_time) from y4a_erp.y4a_erp_sel_exp_ads_inv_log)
	and (c.pims_company in ('Y4A','Hoang Thong', 'General Union','JTO')  or c.pims_company is null)
group by
	1,
	2,
	3,
	4,
	5,
	6),
final as(
select
	coalesce(a.platform, b.platform) platform,
	coalesce(a.country_code, b.country) country_code,
	coalesce(a.accname, b.account_name) account_name,
	coalesce(a.campaignid, b.campaignid) campaignid,
	coalesce(a.platform_item_id, b.platform_item_id) platform_item_id,
	b.list_invoice_number,
	a.ads_spend,
	a.ads_spend_usd,
	coalesce(b.invoiced_amount,0) invoiced_amount,
	coalesce(b.invoiced_amount_usd,0) invoiced_amount_usd
from
	ads_spend_agg a
full join ads_inv_agg b on
	a.platform = b.platform
	and a.campaignid = b.campaignid
	--	and a.campaignname = b.campaign_name
	and a.platform_item_id = b.platform_item_id
)select
	concat(accounting_period, '-25')::date  posting_date,
--	concat(accounting_period, '-31')::date  posting_date, -- only on Dec
	*,
	case when ads_spend > coalesce(invoiced_amount,0) then ads_spend -coalesce(invoiced_amount,0) else 0 end accrual,
	case when ads_spend_usd > coalesce(invoiced_amount_usd,0) then ads_spend_usd -coalesce(invoiced_amount_usd,0) else 0 end accrual_usd,
	now() run_time,
	concat('MKTACRAMZ',country_code,to_char(concat(accounting_period, '-25')::date ,'YYYYMM')) external_doc_no
--	concat('MKTACRAMZ',country_code,to_char(concat(accounting_period, '-31')::date ,'YYYYMM')) external_doc_no -- only on Dec
from
	final
;
--Bước 4: Ingest revert accrual vào bảng GL	
--06/2024: AC yêu cầu remove accrual ads cho thị trường SGP (Vì giá trị nhỏ)
insert into y4a_erp.y4a_erp_prod_selling_expense_incremental
with revert_accrual as(
select
	posting_date ,
	external_doc_no ,
	platform ,
	country_code country,
	round(sum(accrual_usd)::numeric,2) accrual_amt_usd
from
	y4a_erp.tb_pnl_sellin_amz_acr_ads
where
	to_char(posting_date, 'YYYY-MM') = to_char((concat(accounting_period,'-01')::date - interval '1 MONTH'),'YYYY-MM')
group by
	1,
	2,
	3,
	4
having
	sum(accrual_usd) != 0
)select
	concat(accounting_period, '-25')::date posting_date,
--	concat(accounting_period, '-31')::date posting_date, -- only Dec
	''due_date,
	''document_type,
	''document_no,
	''bal_vat_bus_posting_group,
	''vat_prod_posting_group,
	'G/L Account'account_type,
	'33510102'account_no,
	'Accrued expenses_Short-term_Operation expenses'account_name,
	'' posting_group,
	concat('AVC ', case when country = 'ARE' then 'UAE' when country = 'MEX' then 'MXN' else country end,': Booking revert accrual Advertising expenses at ', to_char(concat(accounting_period, '-25')::date ,'MonYYYY') ) descriptions,
	''gen_prod_posting_group,
	accrual_amt_usd  amount,
	'G/L Account'bal_account_type,
	'64160103'bal_account_no,
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
	null document_date,
	null exporting_time,
	2 is_processed,
	null error_type,
	'f4acc907-580b-ee11-8f6e-000d3aa09c10'y4a_company_id,
	'b0f0b2b9-0a61-ee11-8df1-00224858547d' journal_id,
	'Y4ALLC'original_y4a_company_id,
	''accounts,
	'DATA-USA'batch_name,
	concat('ADV-AVC ',case when country = 'ARE' then 'UAE' when country = 'MEX' then 'MXN' else country end,'_AMZ') other_name
from
	revert_accrual
where country !~ 'SGP';
--Bước 5: Ingest accrual vào bảng GL
insert into y4a_erp.y4a_erp_prod_selling_expense_incremental
with accrual as(
select
	posting_date ,
	external_doc_no ,
	platform ,
	country_code country,
	round(sum(accrual_usd)::numeric,2) accrual_amt_usd
from
	y4a_erp.tb_pnl_sellin_amz_acr_ads
where
	to_char(posting_date, 'YYYY-MM')::text = accounting_period
group by
	1,
	2,
	3,
	4
having
	sum(accrual_usd) != 0
)select
	 posting_date,
	''due_date,
	''document_type,
	''document_no,
	''bal_vat_bus_posting_group,
	''vat_prod_posting_group,
	'G/L Account'account_type,
	'64160103'account_no,
	'Selling exp_Marketing'account_name,
	'' posting_group,
	concat('AVC ', case when country = 'ARE' then 'UAE' when country = 'MEX' then 'MXN' else country end,': Booking accrual Advertising expenses at ', to_char(posting_date,'MonYYYY') ) descriptions,
	''gen_prod_posting_group,
	accrual_amt_usd  amount,
	'G/L Account'bal_account_type,
	'33510102'bal_account_no,
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
	null document_date,
	null exporting_time,
	2 is_processed,
	null error_type,
	'f4acc907-580b-ee11-8f6e-000d3aa09c10'y4a_company_id,
	'b0f0b2b9-0a61-ee11-8df1-00224858547d' journal_id,
	'Y4ALLC'original_y4a_company_id,
	''accounts,
	'DATA-USA'batch_name,
	concat('ADV-AVC ',case when country = 'ARE' then 'UAE' when country = 'MEX' then 'MXN' else country end,'_AMZ') other_name
from
	accrual
where country !~ 'SGP';
--==============================================================================WALMART DSV====================================================================================================
--Bước 1: Ingest data accrual vào  y4a_erp.tb_pnl_sellin_wmdsv_acr_ads
	
insert into y4a_erp.tb_pnl_sellin_wmdsv_acr_ads
select
	concat('MKTACRWMDSV',to_char(concat(accounting_period,'-25')::date,'YYYYMM')) external_doc_no,
	concat(accounting_period,'-25')::date posting_date,
--	concat('MKTACRWMDSV',to_char(concat(accounting_period,'-31')::date,'YYYYMM')) external_doc_no, -- only Dec
--	concat(accounting_period,'-31')::date posting_date, -- only Dec
	report_date ,
	case
		when accname in ('WM 1P','WM 1S') then 'CHAN-WMDSV'
		else 'CHAN-WMMKP'
	end as channel,
	team,
	sku,
	sum(spend_usd)::numeric(10,
	2)as amount_spend,
	sum(unitssold14d) as quantity_sold
from
	y4a_analyst.y4a_agg_ads_perf_by_sku
where
	platform = 'WM'
	and accname in ('WM 1P','WM 1S')
	and report_date between concat(to_char(concat(accounting_period, '-25')::date - interval '1 MONTH','YYYY-MM'),'-26')::date 
		and concat(accounting_period, '-25')::date
--		and concat(accounting_period, '-31')::date -- only Dec
group by
	1,
	2,
	3,
	4,
	5,
	6;
	
--Bước 2: Ingest revert accrual vào bảng GL
insert into y4a_erp.y4a_erp_prod_selling_expense_incremental
with revert_accrual as(
select
	posting_date,
	external_doc_no,
	sum(amount_spend) accrual_amt_usd,
	'USA' country
from
	y4a_erp.tb_pnl_sellin_wmdsv_acr_ads
where
	to_char(posting_date, 'YYYY-MM') = to_char(concat(accounting_period, '-01')::date - interval '1 month','YYYY-MM')
group by
	1,
	2 )
	select
	concat(accounting_period, '-25')::date posting_date,
--	concat(accounting_period, '-31')::date posting_date, -- only Dec
	'' due_date,
	'' document_type,
	'' document_no,
	'' bal_vat_bus_posting_group,
	'' vat_prod_posting_group,
	'G/L Account' account_type,
	'33510102' account_no,
	'Accrued expenses_Short-term_Operation expenses' account_name,
	'' posting_group,
	concat('WM DSV ', ': Booking revert accrual Advertising expenses at ', to_char(concat(accounting_period, '-25')::date, 'MonYYYY') ) descriptions,
--	concat('WM DSV ', ': Booking revert accrual Advertising expenses at ', to_char(concat(accounting_period, '-31')::date, 'MonYYYY') ) descriptions, -- only Dec
	'' gen_prod_posting_group,
	accrual_amt_usd amount,
	'G/L Account' bal_account_type,
	'64160103' bal_account_no,
	'' bal_vat_prod_posting_group,
	'' bal_gen_prod_posting_group,
	'' currency_code,
	'CHAN-WMDSV' channel_code,
	concat('REG-', case when country = 'ARE' then 'UAE' when country = 'MEX' then 'MXN' else country end) region_code ,
	'PLF-WM-1' platform_code,
	external_doc_no external_document_no,
	'' company_ic_code,
	now() data_updated_time,
	external_doc_no original_invoice_company,
	null document_date,
	null exporting_time,
	2 is_processed,
	null error_type,
	'f4acc907-580b-ee11-8f6e-000d3aa09c10' y4a_company_id,
	'b0f0b2b9-0a61-ee11-8df1-00224858547d' journal_id,
	'Y4ALLC' original_y4a_company_id,
	'' accounts,
	'DATA-USA' batch_name,
	'ADVS-WMDSV' other_name
from
	revert_accrual;
--Bước 3: Ingest accrual vào bảng GL
insert into y4a_erp.y4a_erp_prod_selling_expense_incremental
with accrual as(
select
	posting_date,
	external_doc_no,
	sum(amount_spend) accrual_amt_usd,
	'USA' country
from
	y4a_erp.tb_pnl_sellin_wmdsv_acr_ads
where
	to_char(posting_date, 'YYYY-MM')::text = accounting_period
group by
	1,
	2 )
	select
	posting_date,
	''due_date,
	''document_type,
	''document_no,
	''bal_vat_bus_posting_group,
	''vat_prod_posting_group,
	'G/L Account'account_type,
	'64160103'account_no,
	'Selling exp_Marketing'account_name,
	'' posting_group,
	concat('WM DSV ',': Booking accrual Advertising expenses at ', to_char(concat(accounting_period, '-25')::date,'MonYYYY') ) descriptions,
--	concat('WM DSV ',': Booking accrual Advertising expenses at ', to_char(concat(accounting_period, '-31')::date,'MonYYYY') ) descriptions,
	''gen_prod_posting_group,
	accrual_amt_usd  amount,
	'G/L Account'bal_account_type,
	'33510102'bal_account_no,
	''bal_vat_prod_posting_group,
	''bal_gen_prod_posting_group,
	''currency_code,
	'CHAN-WMDSV'channel_code,
	concat('REG-', case when country = 'ARE' then 'UAE' else country end) region_code ,
	'PLF-WM-1' platform_code,
	external_doc_no external_document_no,
	'' company_ic_code,
	now() data_updated_time,
	external_doc_no original_invoice_company,
	null document_date,
	null exporting_time,
	2 is_processed,
	null error_type,
	'f4acc907-580b-ee11-8f6e-000d3aa09c10' y4a_company_id,
	'b0f0b2b9-0a61-ee11-8df1-00224858547d' journal_id,
	'Y4ALLC' original_y4a_company_id,
	'' accounts,
	'DATA-USA' batch_name,
	'ADVS-WMDSV' other_name
from
	accrual;
--===============================================================================GG Ads===================================================================================================================
--====================================================GG Ads=================================================================
--Bước 1: Ingest accrual detail:
insert into y4a_erp.tb_pnl_sellin_gg_ads_acr_ads
select 
	concat('MKTACRGGADS',to_char(concat(accounting_period,'-25')::date,'YYYYMM')) external_doc_no,
	concat(accounting_period,'-25')::date posting_date,
--	concat('MKTACRGGADS',to_char(concat(accounting_period,'-31')::date,'YYYYMM')) external_doc_no, -- only Dec
--	concat(accounting_period,'-31')::date posting_date, -- only Dec
 segments_date::date as report_date,
 de_accountid,
 case
 when de_accountid = '312-511-6688' then 'Sporting Goods'::text
 when de_accountid = '232-008-9078' then 'Yes4all Canada'::text
 when de_accountid = '239-490-5374' then 'Innonet - AMZ US'::text
 when de_accountid = '294-892-8901' then 'Furniture'::text
 else de_accountid
end as gg_account_name,
 campaign_name as campaign,
 campaign_id,
 campaign_advertisingchanneltype as campaign_type,
 adgroup_name as ad_group,
 adgroup_id as ad_group_id,
 adgroup_type as ad_group_type,
 customer_currencycode as currency_code,
 run_date,
 metrics_costmicros/1000000 as ad_cost,
 metrics_impressions as impr,
 metrics_clicks as clicks,
 now() data_updated_time
 from y4a_cdm.y4a_dwa_gga_ads_add
where segments_date between concat(to_char(concat(accounting_period, '-25')::date - interval '1 MONTH','YYYY-MM'),'-26')::date 
	and concat(accounting_period, '-25')::date
--	and concat(accounting_period, '-31')::date -- only Dec
;
--Bước 2: Revert Accrual
insert into y4a_erp.y4a_erp_prod_selling_expense_incremental
with accrual as(
select
	external_doc_no,
	posting_date,
	sum(ad_cost) accrual_amt_usd,
	'USA' country
from
	y4a_erp.tb_pnl_sellin_gg_ads_acr_ads
where to_char(posting_date,'YYYY-MM') = to_char(concat(accounting_period, '-01')::date - interval '1 month','YYYY-MM')
group by
	1,
	2)
	select
	concat(accounting_period,'-25')::date posting_date,
--	concat(accounting_period,'-31')::date posting_date, -- only Dec
	''due_date,
	''document_type,
	''document_no,
	''bal_vat_bus_posting_group,
	''vat_prod_posting_group,
	'G/L Account'account_type,
	'33510102' account_no,
	'Selling exp_Marketing'account_name,
	'' posting_group,
	concat('GG ADS ',': Booking revert accrual Advertising expenses at ', to_char(concat(accounting_period,'-25')::date,'MonYYYY') ) descriptions,
--	concat('GG ADS ',': Booking revert accrual Advertising expenses at ', to_char(concat(accounting_period,'-31')::date,'MonYYYY') ) descriptions, -- only Dec
	''gen_prod_posting_group,
	accrual_amt_usd  amount,
	'G/L Account'bal_account_type,
	'64160103'bal_account_no,
	''bal_vat_prod_posting_group,
	''bal_gen_prod_posting_group,
	''currency_code,
	''channel_code,
	concat('REG-', case when country = 'ARE' then 'UAE' else country end) region_code ,
	'' platform_code,
	external_doc_no external_document_no,
	'' company_ic_code,
	now() data_updated_time,
	external_doc_no original_invoice_company,
	null document_date,
	null exporting_time,
	2 is_processed,
	null error_type,
	'f4acc907-580b-ee11-8f6e-000d3aa09c10' y4a_company_id,
	'b0f0b2b9-0a61-ee11-8df1-00224858547d' journal_id,
	'Y4ALLC' original_y4a_company_id,
	'' accounts,
	'DATA-USA' batch_name,
	'ADV-AVC USA_GGA' other_name
from
	accrual;
--Bước 3: Book Accrual
insert into y4a_erp.y4a_erp_prod_selling_expense_incremental
with accrual as(
select
	external_doc_no,
	posting_date,
	sum(ad_cost) accrual_amt_usd,
	'USA' country
from
	y4a_erp.tb_pnl_sellin_gg_ads_acr_ads
where to_char(posting_date,'YYYY-MM')::text = accounting_period
group by
	1,
	2)
	select
	posting_date,
	''due_date,
	''document_type,
	''document_no,
	''bal_vat_bus_posting_group,
	''vat_prod_posting_group,
	'G/L Account'account_type,
	'64160103'account_no,
	'Selling exp_Marketing'account_name,
	'' posting_group,
	concat('GG ADS ',': Booking accrual Advertising expenses at ', to_char(concat(accounting_period,'-25')::date,'MonYYYY') ) descriptions,
--	concat('GG ADS ',': Booking accrual Advertising expenses at ', to_char(concat(accounting_period,'-31')::date,'MonYYYY') ) descriptions, -- only Dec
	''gen_prod_posting_group,
	accrual_amt_usd  amount,
	'G/L Account'bal_account_type,
	'33510102'bal_account_no,
	''bal_vat_prod_posting_group,
	''bal_gen_prod_posting_group,
	''currency_code,
	''channel_code,
	concat('REG-', case when country = 'ARE' then 'UAE' else country end) region_code ,
	'' platform_code,
	external_doc_no external_document_no,
	'' company_ic_code,
	now() data_updated_time,
	external_doc_no original_invoice_company,
	null document_date,
	null exporting_time,
	2 is_processed,
	null error_type,
	'f4acc907-580b-ee11-8f6e-000d3aa09c10' y4a_company_id,
	'b0f0b2b9-0a61-ee11-8df1-00224858547d' journal_id,
	'Y4ALLC' original_y4a_company_id,
	'' accounts,
	'DATA-USA' batch_name,
	'ADV-AVC USA_GGA' other_name
from
	accrual;
	END;
$procedure$
;

