-- DROP PROCEDURE y4a_erp.sp_y4a_erp_sel_exp_inv_api_incr();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_y4a_erp_sel_exp_inv_api_incr()
LANGUAGE plpgsql
AS $procedure$
	BEGIN
TRUNCATE TABLE y4a_erp.y4a_erp_prod_selling_expense_incremental_daily;
INSERT INTO y4a_erp.y4a_erp_prod_selling_expense_incremental_daily
(
SELECT * FROM y4a_erp.y4a_erp_prod_selling_expense_incremental
WHERE posting_date < date_trunc('month',now()::date + INTERVAL '1 MONTH')
);
UPDATE y4a_erp.y4a_erp_prod_selling_expense_incremental_daily
SET is_processed = 2;
INSERT INTO y4a_erp.y4a_erp_prod_selling_expense_incremental_daily
	WITH latest AS
			(
			SELECT * FROM y4a_erp.tb_y4a_erp_sel_exp_inv_api_log
			WHERE data_updated_time = (SELECT max(data_updated_time) FROM y4a_erp.tb_y4a_erp_sel_exp_inv_api_log )
			AND document_date <date_trunc('month',now()::date + INTERVAL '1 MONTH')
			)
SELECT a.* FROM latest AS a
LEFT JOIN (SELECT * FROM y4a_erp.y4a_erp_prod_selling_expense_incremental_daily WHERE document_date <date_trunc('month',now()::date + INTERVAL '1 MONTH')) AS  b
ON  a.external_document_no = b.external_document_no AND a.region_code = b.region_code
WHERE b IS NULL
AND a.amount !=0
AND a.posting_date <date_trunc('month',now()::date + INTERVAL '1 MONTH');
	END;
$procedure$
;

