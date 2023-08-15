def rds_python_conn(sql,table_name):
    import psycopg2
    import json
    from sqlalchemy import create_engine
    # rds와 python 연동을 위한 정보 입력
    with open('postgres.info.json','r') as json_file:
        db_info = json.load(json_file)

    host = db_info.get('host')
    dbname = 'postgres' 
    user = 'postgres' 
    password = db_info.get('password')
    port = 5432

    connection = psycopg2.connect(host=host,
                                  dbname=dbname ,
                                  user=user ,
                                  password=password ,
                                  port=port)

    cur = connection.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table_name};") 
    cur.execute(sql)
    connection.commit()

def data_store(dataframe,table_name):
    import json
    from sqlalchemy import create_engine
    # rds와 python 연동을 위한 정보 입력
    with open('postgres.info.json','r') as json_file:
        db_info = json.load(json_file)

    host = db_info.get('host')
    dbname = 'postgres' 
    user = 'postgres' 
    password = db_info.get('password')
    port = 5432

    # SQLAlchemy 엔진 생성
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

    # 데이터프레임을 데이터베이스에 적재
    dataframe.to_sql(name=table_name, con=engine, if_exists='replace', index=False)


