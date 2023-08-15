from products import products_data
from seller import seller, processing, processing_other_func
from geolocation import geolocation_data
from order import order_data
from rds_python_connect import data_store
from seller import only_seller

# 주문
order = order_data()
# dateframe, mapping_order_id = processing(order, 'order_id')
# dateframe1, mapping_customer_id = processing(dateframe, 'customer_id')
# dateframe2, mapping_order_item_id = processing(dateframe1, 'order_item_id')
# dateframe3, mapping_product_id = processing(dateframe2, 'product_id')
# dateframe4, mapping_seller_id = processing(dateframe3, 'seller_id')
# dateframe5, mapping_customer_unique_id = processing(dateframe4, 'customer_unique_id')
data_store(order,'order_table_JS')

# 프로덕트
products = products_data()
# dateframe_pro = processing_other_func(products,'product_id',mapping_product_id)
data_store(products,'products_table_JS')

# 판매자
lead_table = seller()
# dateframe_seller1 = processing_other_func(seller_table,'seller_id',mapping_seller_id)
# dateframe_seller2, mapping_mql_id = processing(dateframe_seller1, 'mql_id')
# dateframe_seller3, mapping_sdr_id = processing(dateframe_seller2, 'sdr_id')
# dateframe_seller4, mapping_sr_id = processing(dateframe_seller3, 'sr_id')
# dateframe_seller5, mapping_landing_page_id = processing(dateframe_seller4, 'landing_page_id')
data_store(lead_table,'lead_table_JS')

# 위치
geolocation = geolocation_data()
data_store(geolocation,'geolocation_table_JS')

onlyseller = only_seller()
data_store(onlyseller,'only_seller_JS')