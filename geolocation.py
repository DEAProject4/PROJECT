import pandas as pd
import os
from rds_python_connect import data_store
from seller import processing


# 찾는 경로
search_path2 = 'Brazilian E-Commerce Public Dataset by Olist'

# 파일명
target_file1 = 'olist_geolocation_dataset.csv'


# 잠재 고객 중 체결 안된 고객테이블
def geolocation_data(selection='N'):
    global search_path2
    global target_file1

     # 파일 경로 생성
    file_path1 = os.path.join(search_path2, target_file1)

    # CSV 파일 읽어오기
    geolocation = pd.read_csv(file_path1)

    #"Y"이면 파일 생성.
    if selection !='N':
        geolocation.to_csv('geolocation.csv',index=False)

    return geolocation