-- DROP PROCEDURE y4a_erp.sp_y4a_erp_sel_exp_inv_api_log();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_y4a_erp_sel_exp_inv_api_log()
LANGUAGE plpgsql
AS $procedure$
	BEGIN
INSERT INTO y4a_erp.tb_y4a_erp_sel_exp_inv_api_log(
WITH full_inv AS
(
			(
					WITH cop_inv AS
					(
					SELECT
						invoice_number,
						invoice_date,
						funding_type ,
						sub_funding_type ,
						currency ,
						a.country,
						CASE
								WHEN (b.pims_company IN ('Y4A','Hoang Thong','General Union','JTO')
								OR b.pims_company IS NULL )
							THEN 'Y4A' ELSE b.pims_company END AS pims_company,
						booked_type ,
						sum(quantity) AS quantity,
						sum(cop_fee_by_sku) AS amount
					FROM
						y4a_erp.tb_y4a_erp_sel_exp_cop_inv_update a
						LEFT JOIN y4a_finance.dim_pims_platform_product_profile_by_country  b
										ON a.asin = b.platform_prd_id  AND b.platform ='Amazon' AND a.country =b.country
						WHERE invoice_number NOT IN (SELECT DISTINCT original_invoice  FROM y4a_erp.y4a_erp_sel_exp_inv)--- Bảng cũ trước khi lên được API. chạy ở bảng cũ được 2 tháng
					GROUP BY
						1,
						2,
						3,
						4,
						5,
						6,
						7,8
					)
					SELECT
						invoice_date::date AS posting_date,
						NULL AS due_date,
						NULL AS document_type,
						NULL AS document_no,
						'' AS bal_vat_bus_posting_group,
						'' AS vat_prod_posting_group,	
						'G/L Account' account_type,
							CASE WHEN a.pims_company = 'Y4A'	
							THEN concat('CHAN-AVC', '-', a.country, ': Booking Promotions exps, Inv# ', a.invoice_number, '_', 'Y4A','_',to_char(invoice_date, 'MONYYYY'))
							ELSE concat('OBH','-','CHAN-AVC', '-', a.country, ': Booking Promotions exps, Inv# ', a.invoice_number, '_', a.pims_company, '_', to_char(invoice_date, 'MONYYYY')) END AS descriptions,
							'' AS gen_prod_posting_group,
							round(a.amount::NUMERIC,2) AS amount,
							'' AS bal_vat_prod_posting_group,
							'' AS bal_gen_prod_posting_group,
							a.currency AS currency_code,
							'CHAN-AVC' AS channel_code,
							t3.erp AS region_code,
							'PLF-AMZ-1' AS platform_code,
							invoice_number AS external_document_no,
							'' AS Company_ic_Code,
							CASE WHEN a.pims_company = 'Y4A'	
							THEN concat(invoice_number, '_', 'Y4A')
							ELSE concat(invoice_number, '_',  a.pims_company) END AS original_invoice,
							invoice_date::date AS document_date,
						NULL AS exporting_time,
						2 AS is_processed,
						NULL AS error_type,
						 t5.y4a_company_id  AS y4a_company_id,
						CASE WHEN a.pims_company = 'Y4A'	THEN  id
						 ELSE 'edecd882-3993-ee11-be36-6045bd1c144b' END AS journal_id,
						t5.original_y4a_company_id,
						NULL AS accounts,
						t5.displayname AS batch_name
					FROM
						cop_inv a
					LEFT JOIN y4a_erp.tb_y4a_erp_dimension t3 ON
						a.country = t3.db
					left  JOIN
							(
								SELECT b.original_y4a_company_id,a.y4a_company_id,a.id,a.code,regexp_matches(code,'.*-.*'),displayname FROM y4a_system.bc_general_journal a
								LEFT JOIN (SELECT DISTINCT y4a_company_id,original_y4a_company_id FROM y4a_erp.y4a_erp_prod_master_data_item) b
								ON a.y4a_company_id = b.y4a_company_id
							) t5
							ON t5.original_y4a_company_id ='Y4ALLC' AND a.currency = split_part(t5.code ,'-',1)
						WHERE invoice_number NOT IN ( SELECT invoice_id  FROM y4a_cdm.y4a_dwc_amz_cop_ovr
					 WHERE withdraw_time::date <'2023-10-01')
					 AND invoice_number NOT IN (SELECT DISTINCT original_invoice  FROM y4a_erp.y4a_erp_sel_exp_inv)
			)
						UNION ALL
			(
							WITH ads_inv as(
								SELECT
								invoice_number,
								invoice_date,
								acc_id AS accname,
								country,
								currency,
								CASE
										WHEN (company IN ('Y4A','Hoang Thong','General Union')
									OR company IS NULL) THEN  'Y4A' ELSE company END AS company,
								channel,
								platform,
								account_name AS account,
								original_y4a_company_id,
								sum(invoice_amount) AS amount
							FROM
								y4a_erp.tb_y4a_erp_sel_exp_ads_dtl_update
							WHERE invoice_number NOT IN (SELECT DISTINCT original_invoice  FROM y4a_erp.y4a_erp_sel_exp_inv)
								GROUP BY 1,2,3,4,5,6,7,8,9,10
								)
								SELECT
								invoice_date::date AS posting_date,
								'' AS due_date,
								'' AS document_type,
								'' AS document_no,
								'' AS bal_vat_bus_posting_group,
								'' AS vat_prod_posting_group,
									CASE
										WHEN a.company = 'Y4A'  THEN 'G/L Account'
									ELSE 'G/L Account'
								END AS 	account_type,
									CASE WHEN a.company = 'Y4A' THEN concat(t6.erp, '-', a.country, ': Selling exp_Marketing, Inv# ', a.invoice_number, '_', 'Y4A','_',to_char(invoice_date, 'MONYYYY'))
									ELSE concat('OBH','-',t6.erp, '-', a.country, ': Selling exp_Marketing, Inv# ', a.invoice_number, '_', a.company, '_', to_char(invoice_date, 'MONYYYY')) END AS descriptions,
									NULL AS gen_prod_posting_group,
									round(a.amount::NUMERIC,2) AS amount,
								'' AS bal_vat_prod_posting_group,
								'' AS bal_gen_prod_posting_group,
								currency  AS currency_code,
									t6.erp AS channel_code,
									t3.erp AS region_code,
									t4.erp AS platform_code,
									invoice_number AS external_document_no,
									'' AS Company_ic_Code,
									CASE WHEN a.company = 'Y4A' THEN  concat(invoice_number, '_', 'Y4A')
									ELSE concat(invoice_number, '_',  a.company) END AS original_invoice,
									invoice_date::date AS document_date,
								NULL AS exporting_time,
								CASE
										WHEN (country !='SGP') THEN 2
										ELSE 3 END AS is_processed,
					NULL AS error_type,
						t5.y4a_company_id AS y4a_company_id,
						CASE WHEN a.company = 'Y4A'	THEN  id
						 ELSE 'edecd882-3993-ee11-be36-6045bd1c144b' END AS journal_id,
						t5.original_y4a_company_id,
						account AS accounts,
						t5.displayname AS batch_name
							FROM
								 ads_inv a
							LEFT JOIN y4a_erp.tb_y4a_erp_dimension t3 ON
								a.country = t3.db
							LEFT JOIN y4a_erp.tb_y4a_erp_dimension AS t4 ON
								a.platform = t4.db
							LEFT JOIN y4a_erp.tb_y4a_erp_dimension AS t6 ON
								a.channel = t6.db
							left  JOIN
									(
										SELECT b.original_y4a_company_id,a.y4a_company_id,a.id,a.code,regexp_matches(code,'.*-.*'),displayname FROM y4a_system.bc_general_journal a
										LEFT JOIN (SELECT DISTINCT y4a_company_id,original_y4a_company_id FROM y4a_erp.y4a_erp_prod_master_data_item) b
										ON a.y4a_company_id = b.y4a_company_id
									) t5
							ON t5.original_y4a_company_id =a.original_y4a_company_id AND a.currency = split_part(t5.code ,'-',1)
				)
	),
	full_formatted AS
	(
SELECT
	posting_date,
	due_date,
	document_type,
	document_no,
	bal_vat_bus_posting_group,
	vat_prod_posting_group,
	a.account_type,
	b.account_no,
	b.account_name,
	b.posting_group,
	descriptions,
	gen_prod_posting_group,
	amount::numeric,
	b.bal_account_type,
	b.bal_account_no,
	bal_vat_prod_posting_group,
	bal_gen_prod_posting_group,
	a.currency_code,
	a.channel_code,
	a.region_code,
	a.platform_code,
	a.external_document_no,
	a.company_ic_code,
	now() AS data_updated_time,
	original_invoice,
	document_date,
	exporting_time,
	is_processed,
	error_type,
	a.y4a_company_id,
	CASE WHEN original_invoice NOT LIKE '%Y4A' THEN 'edecd882-3993-ee11-be36-6045bd1c144b'
	ELSE a.journal_id END AS journal_id ,
	a.original_y4a_company_id,
	a.accounts,
	a.batch_name,
	b.other_name
FROM
	full_inv a
LEFT JOIN y4a_erp.bc_sel_exp_master_dim_for_gl_account_api b -- bang UPDATE bang file EXCEL !!???
ON split_part(a.original_invoice,'_',2) = b.company AND a.account_type =b.account_type AND position(split_part(b.account_name,'_',2) IN a.descriptions)>0
AND a.currency_code = b.currency_code AND a.channel_code = b.channel_code AND a.region_code =b.region_code  AND a.original_y4a_company_id = b.original_y4a_company_id
AND a.platform_code = b.platform_code
where original_invoice LIKE '%Y4A'
	UNION ALL
	SELECT
	posting_date,
	due_date,
	document_type,
	document_no,
	bal_vat_bus_posting_group,
	vat_prod_posting_group,
	a.account_type,
	b.account_no,
	b.account_name,
	b.posting_group,
	descriptions,
	gen_prod_posting_group,
	amount::numeric,
	b.bal_account_type,
	b.bal_account_no,
	bal_vat_prod_posting_group,
	bal_gen_prod_posting_group,
	a.currency_code,
	a.channel_code,
	a.region_code,
	a.platform_code,
	a.external_document_no,
	a.company_ic_code,
	now() AS data_updated_time,
	original_invoice,
	document_date,
	exporting_time,
	is_processed,
	error_type,
	a.y4a_company_id,
	CASE WHEN original_invoice NOT LIKE '%Y4A' THEN 'edecd882-3993-ee11-be36-6045bd1c144b'
	ELSE a.journal_id END AS journal_id ,
	a.original_y4a_company_id,
	a.accounts,
	a.batch_name,
	b.other_name
FROM
	full_inv a
LEFT JOIN y4a_erp.bc_sel_exp_master_dim_for_gl_account_api b
ON split_part(a.original_invoice,'_',2) = b.company AND a.account_type =b.account_type
AND a.currency_code = b.currency_code AND a.channel_code = b.channel_code AND a.region_code =b.region_code AND a.original_y4a_company_id = b.original_y4a_company_id
AND a.platform_code = b.platform_code
where original_invoice NOT LIKE '%Y4A'
	UNION ALL
	SELECT
	posting_date,
	due_date,
	document_type,
	document_no,
	bal_vat_bus_posting_group,
	vat_prod_posting_group,
	b.account_type,
	b.account_no,
	b.account_name,
	b.posting_group,
	descriptions,
	gen_prod_posting_group,
	- amount::NUMERIC AS amount,
	b.bal_account_type,
	b.bal_account_no,
	bal_vat_prod_posting_group,
	bal_gen_prod_posting_group,
	a.currency_code,
	a.channel_code,
	a.region_code,
	a.platform_code,
	a.external_document_no,
	a.company_ic_code,
	now() AS data_updated_time,
	original_invoice,
	document_date,
	exporting_time,
	is_processed,
	error_type,
	a.y4a_company_id,
	CASE WHEN original_invoice NOT LIKE '%Y4A' THEN 'edecd882-3993-ee11-be36-6045bd1c144b'
	ELSE a.journal_id END AS journal_id ,
	a.original_y4a_company_id,
	a.accounts,
	a.batch_name,
	b.other_name
FROM
	full_inv a
LEFT JOIN y4a_erp.bc_sel_exp_master_dim_for_gl_account_api b
ON 'OBH'= b.company AND 'Vendor' =b.account_type
AND a.currency_code = b.currency_code AND a.channel_code = b.channel_code AND a.region_code =b.region_code
AND a.platform_code = b.platform_code
where original_invoice NOT LIKE '%Y4A'
)
SELECT y4a_erp.posting_date(posting_date,now()::date)  AS posting_date,
	due_date,
	document_type,
	concat('GJNLGEN',to_char(posting_date,'YY'),to_char(posting_date,'MM'), (10000 + (DENSE_RANK() OVER (ORDER BY original_invoice))::NUMERIC)::TEXT) AS document_no,
	bal_vat_bus_posting_group,
	vat_prod_posting_group,
	account_type,
	account_no,
	account_name,
	posting_group,
	descriptions,
	gen_prod_posting_group,
	round(amount::NUMERIC,2) AS amount,
	bal_account_type,
	bal_account_no,
	bal_vat_prod_posting_group,
	bal_gen_prod_posting_group,
	CASE WHEN currency_code = 'USD' THEN '' ELSE currency_code END AS currency_code,
	channel_code,
	region_code,
	platform_code,
	external_document_no,
	company_ic_code,
	now() AS data_updated_time,
	original_invoice,
	document_date,
	exporting_time,
	is_processed,
	error_type,
	y4a_company_id,
	CASE WHEN original_invoice NOT LIKE '%Y4A' THEN 'edecd882-3993-ee11-be36-6045bd1c144b'
	ELSE journal_id END AS journal_id ,
	original_y4a_company_id,
	accounts,
	batch_name ,
	other_name
	FROM full_formatted
)
;
	END;
$procedure$
;

