import pandas as pd
import os
from rds_python_connect import data_store
from seller import processing
from functools import reduce


# 찾는 경로
search_path2 = 'Brazilian E-Commerce Public Dataset by Olist'

# 파일명
target_file1 = 'olist_orders_dataset.csv'
target_file2 = 'olist_order_payments_dataset.csv'
target_file3 = 'olist_order_items_dataset.csv'
target_file4 = 'olist_customers_dataset.csv'


# 잠재 고객 중 체결 안된 고객테이블
def order_data(selection='N'):
    global search_path2
    global target_file1
    global target_file2
    global target_file3
    global target_file4

     # 파일 경로 생성
    file_path1 = os.path.join(search_path2, target_file1)
    file_path2 = os.path.join(search_path2, target_file2)
    file_path3 = os.path.join(search_path2, target_file3)
    file_path4 = os.path.join(search_path2, target_file4)

    # CSV 파일 읽어오기
    df_olist_orders = pd.read_csv(file_path1)
    df_olist_order_payments = pd.read_csv(file_path2)
    df_olist_order_items = pd.read_csv(file_path3)
    df_olist_customers = pd.read_csv(file_path4)

    # merge
    order_merge_tmp1 = df_olist_orders.merge(df_olist_order_payments, on='order_id', how='left')
    order_merge_tmp2 = order_merge_tmp1.merge(df_olist_order_items, on='order_id', how='left')
    order = order_merge_tmp2.merge(df_olist_customers, on='customer_id', how='left')


    # 전처리
    order['order_purchase_timestamp'] = pd.to_datetime(order['order_purchase_timestamp'])
    order.drop(index=order[(order['order_status']=='delivered')&(order['product_id'].isnull())].index, inplace=True)
    
    #"Y"이면 파일 생성.
    if selection !='N':
        order.to_csv('order.csv',index=False)

    return order

