with pov as 
(select date(i.created_at) as receive_date, i.product_id, i.product_attribute_id, i.warehouse_id,case when i.company_id > 0 then c.code
else po.company_type 
end as comp_type,
sum(i.quantity) as qty_po,
sum(i.quantity*smuh.purchase_price_inc_ppn) as value_po from inventories i 
left join l3_purchasing.s_margin_union_h smuh on smuh.inventory_id = i.id 
left join companies c on c.id = i.company_id 
left join purchase_orders po on po.id = i.new_purchase_order_id 
where i.inventory_id = 0 and i.inventory_vendor_id = 0 and i.transfer_good_id = 0 and i.production_order_id = 0
and i.created_at < '2023-09-01' and i.created_at >= '2023-01-01'
group by 1,2,3,4,5
),
tfv as
(select date(i.created_at) as receive_date, i.product_id, i.product_attribute_id, i.warehouse_id, c.code as comp_type, 
sum(i.quantity) as qty_tf,
sum(i.quantity*smuh.purchase_price_inc_ppn) as value_tf from inventories i 
left join l3_purchasing.s_margin_union_h smuh on smuh.inventory_id = i.id 
left join companies c on c.id = i.company_id 
where i.inventory_id = 0 and i.inventory_vendor_id = 0 and i.transfer_good_id > 0
and i.created_at < '2023-09-01' and i.created_at >= '2023-01-01'
group by 1,2,3,4,5),
ivv as
(select date(i.created_at) as receive_date, i.product_id, i.product_attribute_id, i.inventory_vendor_id, i.warehouse_id, 
case when i.company_id > 0 then c.code
else po.company_type 
end as comp_type,
sum(i.quantity) as qty_ven,
sum(i.quantity*smuh.purchase_price_inc_ppn) as value_ven from inventories i 
left join l3_purchasing.s_margin_union_h smuh on smuh.inventory_id = i.id 
left join companies c on c.id = i.company_id 
left join purchase_orders po on po.id = i.new_purchase_order_id 
where i.inventory_id = 0 and i.inventory_vendor_id > 0 and i.transfer_good_id = 0 and i.production_order_id = 0
and i.created_at < '2023-09-01' and i.created_at >= '2023-01-01'
group by 1,2,3,4,5,6
)
select date(wi.created_at) as "period", wi.product_id, wi.product_attribute_id, wi.warehouse_id,
wi.product_name, wi.unit, w."name" as warehouse,
case when wi.comp_type > 0 then wi.comp_type
when wi.product_id = 5115 and wi.comp_type isnull then 'CMI'
else 'GSA'
end as comp_type, coalesce(sum(total_qty),0) as stock_qty, coalesce(sum(total_purchase_price),0) as stock_value,
max(pov.qty_po) as qty_po, max(value_po) as value_po, max(tfv.qty_tf) as qty_tf, max(value_tf) as value_tf,
sum(ivv.qty_ven) as qty_ven, sum(ivv.value_ven) as value_ven from purchasing.wcs_inventory wi 
left join pov 
on pov.product_id = wi.product_id and pov.product_attribute_id = wi.product_attribute_id 
and pov.warehouse_id = wi.warehouse_id and pov.receive_date = date(wi.created_at) and 
case when wi.comp_type > 0 then wi.comp_type = pov.comp_type 
when wi.comp_type isnull and wi.product_name like '%Buncit%' then pov.comp_type = 'CMI'
else pov.comp_type = 'GSA' end
left join tfv on tfv.product_id = wi.product_id and tfv.product_attribute_id = wi.product_attribute_id 
and tfv.warehouse_id = wi.warehouse_id and tfv.receive_date = date(wi.created_at) and 
case when wi.comp_type > 0 then wi.comp_type = tfv.comp_type 
when wi.comp_type isnull and wi.product_id = 5115 then tfv.comp_type = 'CMI'
else tfv.comp_type = 'GSA' end
left join ivv on ivv.product_id = wi.product_id and ivv.product_attribute_id = wi.product_attribute_id 
and ivv.warehouse_id = wi.warehouse_id and ivv.receive_date = date(wi.created_at) and 
case when wi.comp_type > 0 then wi.comp_type = ivv.comp_type 
when wi.comp_type isnull and wi.product_id = 5115 then ivv.comp_type = 'CMI'
else ivv.comp_type = 'GSA' end
left join warehouses w on w.id = wi.warehouse_id 
where wi.created_at < '2023-09-01' and wi.created_at >= '2023-01-01'
group by 1,2,3,4,5,6,7,8