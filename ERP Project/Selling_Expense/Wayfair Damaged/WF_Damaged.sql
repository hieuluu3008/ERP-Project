-- DROP PROCEDURE y4a_erp.sp_y4a_erp_wayfair_damage();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_y4a_erp_wayfair_damage()
LANGUAGE plpgsql
AS $procedure$
	BEGIN
			insert into y4a_erp.y4a_erp_prod_selling_expense_incremental (
WITH company AS (
SELECT DISTINCT a.invoice_num inv_num, e.belong_to_company
from y4a_cdm.y4a_dwb_wyf_fin_inv_dsv_upt a
left join y4a_cdm.y4a_dwb_wyf_ord_exp_upt b ON a.po_num = b.po_number
left join ( SELECT DISTINCT item_id, original_y4a_company_id, belong_to_company, customer_posting_group
          FROM y4a_erp.y4a_erp_prod_master_data_item) e on
	left(b.item_number,4) = e.item_id
	and e.original_y4a_company_id = 'Y4ALLC'
WHERE 1=1
AND a.invoice_num like '%CM'
AND (invoice_num NOT LIKE  '%CM2%' AND invoice_num NOT LIKE '%SAP%' AND lower(invoice_num) NOT LIKE '%credit%')
AND invoice_num NOT LIKE '%_VIR'
AND invoice_num NOT IN ('CS490983981_CM',
						'CS491481567_CM',
						'CS491527485_CM',
						'CS490765553_CM'
						)
)
select
   y4a_erp.posting_date(invoice_date::date,(now()::date - INTERVAL '1 month')::date)   as posting_date
   , '' as due_date
   , '' as document_type
   , '' 	 as document_no
   , '' as bal_vat_bus_posting_group
   , '' as vat_prod_posting_group
   , 'G/L Account' as account_type
--    , '64160110' as account_no
   , CASE
   	WHEN b.belong_to_company IN ('Y4A', 'Hoang Thong','General Union', 'JTO') THEN '64160110'
   	WHEN b.belong_to_company NOT IN ('Y4A', 'Hoang Thong','General Union', 'JTO') THEN '13890101'
   	ELSE NULL
   END AS account_no
   , '' as account_name
   , '3311-MER' as posting_group
--    , concat('Wayfair : Booking damage 4% expenses of revenue ', to_char(invoice_date,'MonYYYY')) as descriptions
   , CASE
   	WHEN b.belong_to_company IN ('Y4A', 'Hoang Thong','General Union', 'JTO') THEN concat('Wayfair : Booking damage 4% expenses of revenue ', to_char(invoice_date,'MonYYYY'))
   	WHEN b.belong_to_company NOT IN ('Y4A', 'Hoang Thong','General Union', 'JTO') THEN concat('Wayfair : Booking damage 4% OBH ', to_char(invoice_date,'MonYYYY'))
   	ELSE NULL
   END AS descriptions
   , '' as gen_prod_posting_group
   , - a.amount as amount
   , 'Vendor' as bal_account_type
   , '331US-SER0031-WFR' as bal_account_no
   , '' as bal_vat_prod_posting_group
   , '' as bal_gen_prod_posting_group
	, case
		when account_name = 'CAN_CAN_PRSEnterprisesLLC' then 'CAD'
		when account_name = 'PRSEnterprisesLLC' then ''
	  end curreny_code
   , 'CHAN-WAYFAIR' as channel_code
	, case
		when account_name = 'CAN_CAN_PRSEnterprisesLLC' then 'REG-CAN'
		when account_name = 'PRSEnterprisesLLC' then 'REG-USA'
	  end region_code
   , 'PLF-WF-1' as platform_code
   , invoice_num as external_document_no
   , '' as company_ic_code
   , now() as data_updated_time
   , invoice_num as original_invoice_company
   , invoice_date::date as document_date
   , '' as exporting_time
   , 0 as is_processed
   , '' as error_type
   , '1b66f0a5-f614-ee11-8f6e-000d3aa09c10' as y4a_company_id  --1b66f0a5-f614-ee11-8f6e-000d3aa09c10
   , '22e71264-5b5b-ee11-8df1-00224858547d' as journal_id --821166a2-0a61-ee11-8df1-00224858547d
   , 'Y4ALLC' as original_y4a_company_id
   , '' as accounts
   , 'USA-DATA' as batch_name
--    , '' as other_name
   , CASE
   	WHEN b.belong_to_company IN ('Y4A', 'Hoang Thong','General Union', 'JTO') THEN ''
   	WHEN b.belong_to_company NOT IN ('Y4A', 'Hoang Thong','General Union', 'JTO') THEN 'US-MER0002-BLS'
   	ELSE NULL
   END AS other_name
from y4a_cdm.y4a_dwb_wyf_fin_inv_dsv_upt a
LEFT JOIN company b ON a.invoice_num = b.inv_num
where invoice_num like '%CM' --- dấu hiệu invoice wayfair damage 4%
AND split_part(invoice_num,'_',1) IN (
										SELECT distinct original_external_doc_no  FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental
										WHERE 1=1
										AND platform ~ 'WF'
										AND document_type ='Order'
										AND bill_to_customer NOT LIKE '%INTC%'
										AND is_processed = 1
											) --- dòng này đang lấy ra những SI đã đẩy BC
--- dòng này đang lexclude những invoice SE WF Damage đã đẩy BC rồi
AND invoice_num NOT IN (SELECT DISTINCT original_invoice_company FROM y4a_erp.y4a_erp_prod_selling_expense_incremental)
--- dòng này bỏ ra các điều kiện không phải Invoice damage của wayfair
AND (invoice_num NOT LIKE  '%CM2%' AND invoice_num NOT LIKE '%SAP%' AND lower(invoice_num) NOT LIKE '%credit%')
--- dòng này bỏ ra các điều kiện không phải Invoice damage của wayfair
AND invoice_num NOT LIKE '%_VIR'
--- dòng này bỏ ra các điều kiện Invoice damage của wayfair mà kế toán đã book manual từ trước
AND invoice_num NOT IN ('CS490983981_CM',
						'CS491481567_CM',
						'CS491527485_CM',
						'CS490765553_CM'
						)				
AND invoice_date <'2025-02-01' ------ sửa lại thời gian chốt sổ
AND amount <0
);
	END;
$procedure$
;

