-- y4a_erp.view_y4a_erp_sales_invoice_reconcile_wm_dsv_transfer_add_in_dai source
CREATE OR REPLACE VIEW y4a_erp.view_y4a_erp_sales_invoice_reconcile_wm_dsv_transfer_add_in_dai
AS WITH wm_dsv_order AS (
        SELECT DISTINCT y4a_erp_prod_sales_invoice_header_incremental_daily.original_external_doc_no AS purchase_order_id
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
         WHERE 1 = 1 AND y4a_erp_prod_sales_invoice_header_incremental_daily.document_type = 'Order'::text AND y4a_erp_prod_sales_invoice_header_incremental_daily.internal_sales_channel = 'WM DSV'::text
       UNION
        SELECT DISTINCT y4a_erp_prod_sales_invoice_header_incremental.original_external_doc_no AS purchase_order_id
          FROM y4a_erp.y4a_erp_prod_sales_invoice_header_incremental
         WHERE 1 = 1 AND y4a_erp_prod_sales_invoice_header_incremental.document_type = 'Order'::text AND y4a_erp_prod_sales_invoice_header_incremental.internal_sales_channel = 'WM DSV'::text
       UNION
        SELECT DISTINCT wm_dsv_acct_booking_jan_may.purchase_order_id
          FROM y4a_erp.wm_dsv_acct_booking_jan_may
       ), wm_dsv_payment AS (
        SELECT DISTINCT a_1.purchase_order_no,
           a_1.payment_date
          FROM y4a_cdm.y4a_dwc_wm_dso_pmt_his a_1
         WHERE a_1.payment_date >= '2023-06-01'::date AND a_1.payment_date <= (( SELECT max(y4a_dwc_wm_dso_pmt_his.payment_date) AS max
                  FROM y4a_cdm.y4a_dwc_wm_dso_pmt_his))
       ), final AS (
        SELECT a_1.purchase_order_no,
           a_1.payment_date
          FROM wm_dsv_payment a_1
            LEFT JOIN wm_dsv_order b_1 ON a_1.purchase_order_no::text = b_1.purchase_order_id
         WHERE b_1.purchase_order_id IS NULL
       )
SELECT a.sale_channel,
   a.purchase_order_id,
   a.order_date,
   a.order_date_format,
   a.order_line_id,
   a.item_sku,
   a.item_product_name,
   a.weight_value,
   a.weight_unit,
   a.order_line_quantity_unit_of_measurement,
   a.order_line_quantity_amount,
   a.status_date,
   a.itm_line_extra_data,
   a.order_line_status,
   a.status_quantity_unit_of_measurement,
   a.status_quantity_amount,
   a.ship_datetime,
   a.carrier_name,
   a.method_code,
   a.tracking_number,
   a.phone,
   a.estimateddeliverydate,
   a.estimatedshipdate,
   a.postal_address_city,
   a.postal_address_state,
   a.postal_address_postal_code,
   a.postal_address_country,
   a.postal_address_address_type,
   a.methodcode,
   a.payment_types,
   a.pck_first_name,
   a.pck_last_name,
   a.pck_complete_number,
   a.charge_currency,
   a.product_charge_amt,
   a.shipping_charge_amt,
   a.fee_charge_amt,
   a.tax_currency,
   a.product_tax_amt,
   a.shipping_tax_amt,
   a.fee_tax_amt,
   a.itm360_unit_cost,
   a.unit_cost_by_logic,
   a.idx_unit_cost,
   a.itm360_extra_data,
   a.run_date
  FROM y4a_analyst.rp_wm_dsv_order_dtl_by_sku a
    LEFT JOIN final b ON a.purchase_order_id::text = b.purchase_order_no::text
 WHERE b.purchase_order_no IS NOT NULL;

