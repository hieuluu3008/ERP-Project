-- DROP PROCEDURE y4a_erp.sp_y4a_erp_selling_expense_acr_est();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_y4a_erp_selling_expense_acr_est()
LANGUAGE plpgsql
AS $procedure$
	BEGIN
INSERT INTO y4a_erp.y4a_erp_sel_exp_acc_est
(-- get latest data from log
WITH latest AS (
SELECT * FROM y4a_erp.tb_y4a_erp_selling_expense_acr_est_log
WHERE run_time = (SELECT max(run_time) FROM y4a_erp.tb_y4a_erp_selling_expense_acr_est_log)
)
SELECT a.* FROM latest a
LEFT JOIN y4a_erp.y4a_erp_sel_exp_acc_est b
ON a.external_document_no = b.external_document_no AND a.document_date = b.document_date
WHERE 1=1
and b IS NULL
and a.posting_date  >= date_trunc('month',y4a_erp.end_of_last_month(now()::date))::date
AND a.posting_date  < date_trunc('month',now()::date)::date
);
	END;
$procedure$
;

