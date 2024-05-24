with pro_po_rm as 
(
select 	po.is_from,
		impol.production_order_id  as pro_id, 
		impol.production_order_item_id as proi_id,
		i.id as inventory_id,
		i.product_id, 
		i.product_attribute_id,
		pa."conversion" as konversi,
		i.purchase_price,
		proim.quantity as qty_proim,
		im.raw_material_id, 
		im.raw_material_attribute_id,
		MIN(poim.purchase_price) as purchase_price_poim,
		avg(poim.discount/poim.quantity) as discount_poim,
		avg(poim.discount) as discount_poim_v3,
		avg(poim.discount_off/poim.quantity) as discount_off_poim,
		avg(case when po.invoice_discount = 0 then poim.discount_off
		else (poim.sub_total/po.sub_total)*po.invoice_discount 
		end) as diskon_faktur,
		avg(case when po.invoice_discount = 0 then poim.discount_off
		else (poim.sub_total/po.sub_total)*po.invoice_discount 
		end / poim.quantity * proim.quantity) as diskon_faktur_detail
from inventory_materials im 
left join inventory_material_production_order_logs impol on impol.inventory_material_id = im.id 
left join purchase_orders po on po.id = im.purchase_order_id
left join raw_material_attributes rma on rma.id = impol.raw_material_attribute_id 
left join purchase_order_item_materials poim on poim.purchase_order_id = po.id and rma.id = poim.raw_material_attribute_id 
left join raw_materials rm on poim.raw_material_id = rm.id 
left join production_order_items proi on proi.id = impol.production_order_item_id 
left join inventories i on proi.production_order_id = i.production_order_id and proi.product_attribute_id = i.product_attribute_id 
left join production_order_item_materials proim on proim.production_order_item_id = proi.id and proim.raw_material_id = rm.id 
and proim.raw_material_attribute_id = rma.id 
and proi.product_attribute_id = i.product_attribute_id 
left join product_attributes pa on pa.id = i.product_attribute_id 
where poim.deleted_by = 0 and poim.deleted_at is null and i.new_purchase_order_id = 0
group by po.is_from,
		impol.production_order_id, 
		impol.production_order_item_id,
		i.id,
		i.product_id, 
		i.product_attribute_id,
		pa."conversion", 
		i.purchase_price,
		proim.quantity,
		im.raw_material_id, 
		im.raw_material_attribute_id
)
, pro_po as
(
select 	is_from,
		pro_id, 
		proi_id,
		inventory_id,
		product_id, 
		product_attribute_id,
		konversi,
		purchase_price,
		qty_proim,
		sum(purchase_price_poim*qty_proim) as purchase_price_poim,
		sum(discount_poim) as discount_poim,
		sum(discount_poim_v3) as discount_poim_v3,
		sum(discount_off_poim) as discount_off_poim,
		sum(diskon_faktur) as diskon_faktur,
		sum(diskon_faktur_detail) as diskon_faktur_detail
from pro_po_rm
group by is_from,
		pro_id, 
		proi_id,
		inventory_id,
		product_id, 
		product_attribute_id,
		konversi,
		purchase_price,
		qty_proim
)
, pro_pro_is_from as
(
select 	0 as po_id,
		pro_id as pro_id, 
		proi_id as poi_id,
		pro_po.inventory_id,
		konversi,
		is_from,
		purchase_price,
		case when is_from < 3 then max(purchase_price)
		else max(purchase_price) - sum(diskon_faktur_detail)
		end as purchase_price_w_ppn,
		case when is_from < 3 then sum(purchase_price_poim) - sum(discount_poim) - sum(discount_off_poim)
		else sum(purchase_price_poim) - avg(discount_poim_v3) - sum(diskon_faktur_detail)
		end as purchase_price_wo_ppn,
		sum(diskon_faktur_detail) as diskon_faktur,
		sum(qty_proim) as quantity 
from pro_po
group by 1,2,3,4,5,6,7
order by 1,2,3,4,5,6
) 
, po_source as
(
select 	
		poi_id, 
		pro_id, 
		po_id, 
		inventory_id,
		purchase_price,
		avg(purchase_price_w_ppn) as purchase_price_w_ppn,
		sum(purchase_price_wo_ppn)/avg(konversi) as purchase_price_wo_ppn
from pro_pro_is_from
group by 1,2,3,4,5
) 
SELECT distinct o.id as order_id,
oi.id as oi_id, 
ip.id as publish_id,
ps.pro_id as po_id,
ps.inventory_id,
oi.selling_price as sp_w_ppn,
case when oi.ppn_type = 1 then oi.selling_price/1.11
else oi.selling_price end as sp_wo_ppn,
ps.purchase_price_w_ppn as pp_w_ppn,
ps.purchase_price_wo_ppn as pp_wo_ppn,
oi.selling_price - ps.purchase_price_w_ppn as tm_w_ppn,
case when oi.ppn_type = 1 then (oi.selling_price/1.11) - ps.purchase_price_wo_ppn
else oi.selling_price - ps.purchase_price_wo_ppn end as tm_wo_ppn
FROM superapp.orders o
left join order_items oi on o.id = oi.order_id
left join product_attributes pa on oi.product_attribute_id = pa.id 
left join order_logs ol on o.id = ol.order_id and oi.id = ol.order_item_id
left join inventory_published ip on ip.id = ol.inventory_publish_id
join po_source ps on ps.inventory_id = ip.inventory_id
union all
select distinct sd.order_id, sd.oi_id, sd.publish_id, sd.po_id, sd.inventory_id, sd.sp_w_ppn, sd.sp_wo_ppn,
sd.purchase_price_w_ppn as pp_w_ppn, sd.purchase_price_wo_ppn as pp_wo_ppn, sd.sp_w_ppn-sd.purchase_price_w_ppn as tm_w_ppn,
sd.sp_wo_ppn-sd.purchase_price_wo_ppn as tm_wo_ppn
from
(SELECT o.id as order_id, pur.is_from,
oi.id as oi_id, oi.selling_price as sp_w_ppn,
case when oi.ppn_type = 1 then oi.selling_price/1.11
else oi.selling_price end as sp_wo_ppn,
ip.id as publish_id,
pur.po_id,
pur.inventory_id,
case 
	when pur.is_from < 3 then (((sum(pur.pp_po-pur.discount_product)*max(pur.ppn))/sum(qty_po))/pa."conversion") - ((sum(pur.diskon_faktur)/sum(qty_po))/pa."conversion")
	else ((sum(pur.sub_total)/sum(qty_po))/pa."conversion") - ((sum(pur.diskon_faktur)/sum(qty_po))/pa."conversion")
end as purchase_price_w_ppn,
((sum(pp_po)/sum(qty_po))/pa."conversion") - ((sum(pur.discount_product)/sum(qty_po))/pa."conversion") - ((sum(pur.diskon_faktur)/sum(qty_po))/pa."conversion")
purchase_price_wo_ppn
FROM superapp.orders o
left join order_items oi on o.id = oi.order_id
left join product_attributes pa on oi.product_attribute_id = pa.id 
left join order_logs ol on o.id = ol.order_id and oi.id = ol.order_item_id
left join inventory_published ip on ip.id = ol.inventory_publish_id 
join 
(select 
	po.is_from,
	i.new_purchase_order_id as po_id,
	poi.id as poi_id,
	i.id as inventory_id, 
	i.purchase_price as pp_inv,
	case 
		when poi.ppn = 0 then 1.00
		when poi.ppn = 10 then 1.10
		when poi.ppn = 11 then 1.11
	end as ppn,
	case 
		when po.is_from = 3 then poi.discount*poi.quantity  
		else poi.discount 
	end as discount_product,
	case 
		when po.is_from < 3 then poi.discount_off
		when po.grand_total = 0 then 0
		else (poi.sub_total/po.sub_total)*po.invoice_discount 
	end as diskon_faktur,
	sum(poi.purchase_price*poi.quantity) as pp_po,
	sum(poi.sub_total) as sub_total,
	sum(poi.quantity) as qty_po
from inventories i 
left join purchase_orders po on i.new_purchase_order_id = po.id
left join purchase_order_items poi on po.id = poi.purchase_order_id 
and i.product_id = poi.product_id
left join product_attributes pa on pa.id = i.product_attribute_id 
where i.new_purchase_order_id > 0 and poi.deleted_by is null and po.status in (0,1,2)
and i.production_order_id = 0
group by 1,2,3,4,5,6,7,8) pur
on pur.inventory_id = ip.inventory_id
WHERE o.status > 0
and o.created_at > '2022-01-01'
and ol.type = 'order'
and ol.status > 0
group by 
o.id,
oi_id,
oi.selling_price,
oi.ppn_type,
publish_id,
pur.po_id,
pur.inventory_id,
pa."conversion",
pur.is_from,
pur.pp_inv
) sd