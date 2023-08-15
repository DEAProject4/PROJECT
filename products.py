import pandas as pd
import os
from rds_python_connect import data_store
from seller import processing


# 찾는 경로
search_path2 = 'Brazilian E-Commerce Public Dataset by Olist'

# 파일명
target_file1 = 'olist_products_dataset.csv'
target_file2 = 'product_category_name_translation.csv'


# 잠재 고객 중 체결 안된 고객테이블
def products_data(selection='N'):
    global search_path2
    global target_file1
    global target_file2

     # 파일 경로 생성
    file_path1 = os.path.join(search_path2, target_file1)
    file_path2 = os.path.join(search_path2, target_file2)

    # CSV 파일 읽어오기
    products = pd.read_csv(file_path1)
    category = pd.read_csv(file_path2)
    product_cate = products.merge(category, on='product_category_name', how='left')
    product_cate.drop('product_category_name', axis=1, inplace=True)
    product_cate.rename(columns={'product_category_name_english': 'product_category_name'}, inplace=True)
    # processing(products,'product_id')

    #"Y"이면 파일 생성.
    if selection !='N':
        product_cate.to_csv('products.csv',index=False)

    return product_cate