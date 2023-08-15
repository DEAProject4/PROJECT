import pandas as pd
import os
from rds_python_connect import data_store
import json

# 에러메세지 안나오게
import warnings
warnings.filterwarnings('ignore')
from functools import reduce

# 찾는 경로
search_path1 = 'Marketing Funnel by Olist'
search_path2 = 'Brazilian E-Commerce Public Dataset by Olist'

# 파일명
target_file1 = 'olist_closed_deals_dataset.csv'
target_file2 = 'olist_marketing_qualified_leads_dataset.csv'
target_file3 = 'olist_sellers_dataset.csv'


# mql_id나 seller_id 정제하는 함수.
def processing(dataframe,columns):
    mapping = {}

    # 주어진 문자열을 순회하면서 번호 매김
    for item in dataframe[columns]:
        if item in mapping:  # 이미 매핑된 문자인 경우
            pass
        else:  # 처음 나타나는 문자인 경우 새 번호를 부여
            new_number = len(mapping) + 1
            mapping[item] = new_number
        
    try :
        dataframe[columns] = dataframe[columns].apply(lambda x : mapping[x])
    except Exception as e:
        print(e)

    return dataframe, mapping

def processing_other_func(dataframe,columns, mapping):
    try :
        dataframe[columns] = dataframe[columns].apply(lambda x : mapping[x])
    except Exception as e:
        print(e)

    return dataframe


# 잠재 고객 테이블
def seller(selection='N'):
    global search_path1
    global search_path2
    global target_file1
    global target_file2

     # 파일 경로 생성
    file_path1 = os.path.join(search_path1, target_file1)
    file_path2 = os.path.join(search_path1, target_file2)

    # CSV 파일 읽어오기
    df_closed_deals = pd.read_csv(file_path1)
    df_marketing_leads = pd.read_csv(file_path2)

    MQL_tmp =  df_marketing_leads.merge(df_closed_deals, on='mql_id',how='left')
    MQL_tmp['first_contact_date'] = pd.to_datetime(MQL_tmp['first_contact_date'])
    MQL_tmp['won_date'] = pd.to_datetime(MQL_tmp['won_date'])
    MQL_tmp['origin'] = MQL_tmp['origin'].fillna('unknown')
    MQL_tmp['lead_behaviour_profile'] = MQL_tmp['lead_behaviour_profile'].fillna('unknown')
    MQL_tmp['lead_type'] = MQL_tmp['lead_type'].fillna(MQL_tmp['lead_type'].value_counts().max())
    MQL_tmp['business_segment'] = MQL_tmp['business_segment'].fillna(MQL_tmp['business_segment'].value_counts().max())
    MQL_tmp.drop(columns=['has_company','has_gtin','average_stock','declared_product_catalog_size'],axis=1, inplace=True)

    #"Y"이면 파일 생성.
    if selection !='N':
        MQL_tmp.to_csv('MQL_tmp.csv',index=False)

    return MQL_tmp


def only_seller(selection='N'):
    global search_path2
    global target_file3

     # 파일 경로 생성
    file_path1 = os.path.join(search_path2, target_file3)

    # CSV 파일 읽어오기
    df_seller = pd.read_csv(file_path1)


    #"Y"이면 파일 생성.
    if selection !='N':
        df_seller.to_csv('df_seller.csv',index=False)

    return df_seller