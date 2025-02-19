-- y4a_erp.view_y4a_erp_sales_invoice_reconcile_wm_dsv_daily source
CREATE OR REPLACE VIEW y4a_erp.view_y4a_erp_sales_invoice_reconcile_wm_dsv_daily
AS WITH wm_dsv AS (
        WITH item_360 AS (
                SELECT y4a_dwb_wm_sup_cat_itm.supplier_stock_id,
                   string_agg(DISTINCT y4a_dwb_wm_sup_cat_itm.item_id::text, ','::text) AS item_id
                  FROM y4a_cdm.y4a_dwb_wm_sup_cat_itm
                 WHERE 1 = 1 AND y4a_dwb_wm_sup_cat_itm.fulfillment_method::text = 'DSV'::text AND y4a_dwb_wm_sup_cat_itm.site_status::text <> 'UNPUBLISHED'::text
                 GROUP BY y4a_dwb_wm_sup_cat_itm.supplier_stock_id
               ), dim_order AS (
                SELECT z.purchase_order_id,
                   z.line_count,
                   z.success_line,
                   z.acknowledged_line,
                   z.cancelled_line
                  FROM ( SELECT rp_wm_dsv_order_dtl_by_sku.purchase_order_id,
                           count(*) AS line_count,
                           count(
                               CASE
                                   WHEN rp_wm_dsv_order_dtl_by_sku.order_line_status::text = ANY (ARRAY['Delivered'::character varying::text, 'Shipped'::character varying::text]) THEN rp_wm_dsv_order_dtl_by_sku.purchase_order_id
                                   ELSE NULL::character varying
                               END) AS success_line,
                           count(
                               CASE
                                   WHEN rp_wm_dsv_order_dtl_by_sku.order_line_status::text = 'Acknowledged'::text THEN rp_wm_dsv_order_dtl_by_sku.purchase_order_id
                                   ELSE NULL::character varying
                               END) AS acknowledged_line,
                           count(
                               CASE
                                   WHEN rp_wm_dsv_order_dtl_by_sku.order_line_status::text = 'Cancelled'::text THEN rp_wm_dsv_order_dtl_by_sku.purchase_order_id
                                   ELSE NULL::character varying
                               END) AS cancelled_line
                          FROM y4a_analyst.rp_wm_dsv_order_dtl_by_sku
                         WHERE 1 = 1 AND rp_wm_dsv_order_dtl_by_sku.order_date::date >= '2023-06-01'::date AND rp_wm_dsv_order_dtl_by_sku.order_date::date <= now()::date
                         GROUP BY rp_wm_dsv_order_dtl_by_sku.purchase_order_id) z
                 WHERE (z.success_line + z.cancelled_line) = z.line_count AND z.success_line <> 0
               ), promo_sso_sin AS (
                SELECT wm_dsv_rollback_promotion_log.category_name,
                   wm_dsv_rollback_promotion_log.item_id,
                   wm_dsv_rollback_promotion_log.base_item_id,
                   wm_dsv_rollback_promotion_log.walmart_item,
                   wm_dsv_rollback_promotion_log.item_name,
                   wm_dsv_rollback_promotion_log.variant,
                   wm_dsv_rollback_promotion_log.current_cost,
                   wm_dsv_rollback_promotion_log.promo_cost,
                   wm_dsv_rollback_promotion_log.percent_off,
                   wm_dsv_rollback_promotion_log.funding_amount,
                   wm_dsv_rollback_promotion_log.current_inventory,
                   wm_dsv_rollback_promotion_log.content_quality,
                   wm_dsv_rollback_promotion_log.promo_cost_change_description,
                   wm_dsv_rollback_promotion_log.cogs_or_coop,
                   wm_dsv_rollback_promotion_log.supplier_approval_name,
                   wm_dsv_rollback_promotion_log.event,
                   wm_dsv_rollback_promotion_log.promo_start_1,
                   wm_dsv_rollback_promotion_log.promo_end_1,
                   wm_dsv_rollback_promotion_log.walmart_exclusive_promotion,
                   wm_dsv_rollback_promotion_log.big_bet_promotion,
                   wm_dsv_rollback_promotion_log.average_weekly_sales_units_non_promotional,
                   wm_dsv_rollback_promotion_log.expected_lift_percent,
                   wm_dsv_rollback_promotion_log.supplier_notes,
                   wm_dsv_rollback_promotion_log.data_updated_time
                  FROM y4a_analyst.wm_dsv_rollback_promotion_log
                 WHERE wm_dsv_rollback_promotion_log.data_updated_time = (( SELECT max(wm_dsv_rollback_promotion_log_1.data_updated_time) AS max
                          FROM y4a_analyst.wm_dsv_rollback_promotion_log wm_dsv_rollback_promotion_log_1))
               ), promo_dpl AS (
                SELECT 'DPL'::text AS team,
                   tb_gs_wm_promotion_dl.sku_supplier_stock_id AS sku,
                   tb_gs_wm_promotion_dl.funding_amount::numeric AS funding_amount,
                   tb_gs_wm_promotion_dl.promo_start_1::date AS promo_start_1,
                   tb_gs_wm_promotion_dl.promo_end_1::date AS promo_end_1
                  FROM y4a_analyst.tb_gs_wm_promotion_dl
                 WHERE tb_gs_wm_promotion_dl.run_time = (( SELECT max(tb_gs_wm_promotion_dl_1.run_time) AS max
                          FROM y4a_analyst.tb_gs_wm_promotion_dl tb_gs_wm_promotion_dl_1))
               ), wm_dsv AS (
                SELECT a_2.purchase_order_id,
                   a_2.order_line_id,
                   a_2.order_date,
                   a_2.ship_datetime,
                   "left"(a_2.item_sku::text, 4) AS item_sku,
                   a_2.order_line_quantity_amount,
                   a_2.product_charge_amt,
                   a_2.unit_cost_by_logic,
                   a_2.ship_datetime::date AS ship_date,
                       CASE
                           WHEN COALESCE(y.item_id, z.item_id) IS NOT NULL THEN COALESCE(y.item_id, z.item_id)
                           ELSE 'Not found'::text
                       END AS item_id,
                       CASE
                           WHEN (a_2.item_sku::text IN ( SELECT DISTINCT promo_dpl.sku
                              FROM promo_dpl)) THEN a_2.unit_cost_by_logic + COALESCE(c.funding_amount, 0::numeric)
                           ELSE COALESCE(b_1.current_cost, a_2.unit_cost_by_logic)
                       END AS unit_price
                  FROM y4a_analyst.rp_wm_dsv_order_dtl_by_sku a_2
                    LEFT JOIN item_360 y ON a_2.item_sku::text = y.supplier_stock_id::text
                    LEFT JOIN item_360 z ON "left"(a_2.item_sku::text, 4) = z.supplier_stock_id::text
                    LEFT JOIN promo_sso_sin b_1 ON POSITION((b_1.item_id) IN (COALESCE(y.item_id, z.item_id))) > 0 AND a_2.order_date >= b_1.promo_start_1 AND a_2.order_date <= b_1.promo_end_1
                    LEFT JOIN promo_dpl c ON a_2.item_sku::text = c.sku AND a_2.order_date::date >= c.promo_start_1 AND a_2.order_date::date <= c.promo_end_1
                 WHERE 1 = 1 AND a_2.order_date::date >= '2023-06-01'::date AND a_2.order_date::date <= now()::date AND (a_2.purchase_order_id::text IN ( SELECT DISTINCT dim_order.purchase_order_id
                          FROM dim_order)) AND NOT (a_2.purchase_order_id::text IN ( SELECT DISTINCT y4a_erp_prod_sales_invoice_header_incremental.original_external_doc_no
                          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental
                         WHERE y4a_erp_prod_sales_invoice_header_incremental.document_type = 'Order'::text AND y4a_erp_prod_sales_invoice_header_incremental.internal_sales_channel = 'WM DSV'::text))
                 ORDER BY a_2.purchase_order_id
               )
        SELECT a_1.order_date::date AS order_date,
           NULL::date AS posting_date,
           a_1.order_date::date AS document_date,
           'Order'::text AS document_type,
           '131US-MER0002-WM'::text AS bill_to_customer,
           '131US-MER0002-WM'::text AS sell_to_customer,
           'PLF-WM-1'::text AS platform,
           '1311-MER'::text AS customer_posting_group,
           NULL::text AS posting_description,
           '60D'::text AS payment_term,
           a_1.purchase_order_id AS external_doc_no,
           '156-USA-CA'::text AS wh_location,
           NULL::text AS currency,
           'DATAMART_SI'::text AS source_code,
           'CHAN-WMDSV'::text AS sales_channel,
           'REG-USA'::text AS country,
           row_number() OVER (PARTITION BY a_1.purchase_order_id) * 10000 AS line_no,
           'ITEM'::text AS type,
           "left"(a_1.item_sku, 4) AS no,
           NULL::text AS description,
           'pcs'::text AS uom,
           round(a_1.unit_cost_by_logic, 2) AS unit_price,
           NULL::numeric AS line_discount_pct,
           NULL::numeric AS discount,
           a_1.order_line_quantity_amount AS quantity,
           'VATNOT'::text AS vat_product_posting_group,
           round((a_1.order_line_quantity_amount * round(a_1.unit_price, 2)::double precision)::numeric, 2) AS amount,
           NULL::numeric AS vat_amount,
           NULL::text AS batch_seq,
           a_1.purchase_order_id AS po_number,
           true AS is_valid_record,
           now() AS data_updated_time,
           'WM DSV'::text AS internal_sales_channel,
           a_1.purchase_order_id AS original_external_doc_no,
           a_1.item_sku AS asin,
           comp.company_id AS y4a_company_id,
           'Y4ALLC'::text AS original_y4a_company_id,
           'Y4A'::text AS belong_to_company
          FROM wm_dsv a_1
            LEFT JOIN y4a_erp.y4a_erp_dim_y4a_company_id comp ON comp.company_code = 'Y4ALLC'::text
         WHERE 1 = 1
       ), final_wm_dsv AS (
        SELECT wm_dsv.external_doc_no AS y4a_cdm_external_doc_no,
           sum(wm_dsv.amount) AS y4a_cdm_amount
          FROM wm_dsv
         WHERE to_char(wm_dsv.order_date::timestamp with time zone, 'YYYY-MM'::text) = to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM'::text)
         GROUP BY wm_dsv.external_doc_no
       ), bc_wm_dsv AS (
        SELECT a_1.external_doc_no,
           sum(b_1.amount) AS bc_amount
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily a_1
            LEFT JOIN y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily b_1 ON a_1.external_doc_no = b_1.document_no
         WHERE a_1.internal_sales_channel = 'WM DSV'::text
         GROUP BY a_1.external_doc_no
       )
SELECT a.y4a_cdm_external_doc_no,
   a.y4a_cdm_amount,
   b.external_doc_no,
   b.bc_amount
  FROM final_wm_dsv a
    FULL JOIN bc_wm_dsv b ON a.y4a_cdm_external_doc_no::text = b.external_doc_no
 WHERE a.y4a_cdm_amount <> b.bc_amount OR b.external_doc_no IS NULL;

