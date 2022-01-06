# coding: UTF-8
import sqlite3
import pandas as pd

#シーケンスデータファイル
filepath_data_csv = r"data.csv"
#プライマーファイル
filepath_primer_csv = r"primer.csv"
#結果ファイル名
filepath_result_csv = r"result.csv"

#作業DB
filepath_db = r"test.db"
#パターンテーブル
sql_create_pattern = "CREATE TABLE IF NOT EXISTS M_PATTERN( primer name text primary key , groupname text , pattern text , length INTEGER , GC REAL , Tm , stockConcentration , stockLocation , memo)"
#シーケンスデータテーブル
sql_create_data = "CREATE TABLE IF NOT EXISTS M_SEQUENCE(name text primary key, sequence_data text)"

#SQL
sql_select_execute ='''
SELECT
        t1.primer
       ,t1.groupname
       ,t1.pattern
       ,t1.length
       ,t1.GC
       ,t1.Tm
       ,t1.stockConcentration
       ,t1.stockLocation
       ,t1.memo    
       ,t1.primer
       ,group_concat(t2.name, ',') 
  FROM
       m_pattern t1
     , m_sequence t2 
 WHERE
       t2.sequence_data LIKE '%'|| t1.pattern ||'%'
GROUP BY
       t1.primer 
'''
def create_table():
    #DBをに接続してテーブルを作成
    with sqlite3.connect(filepath_db) as conn:
        cur = conn.cursor()
        #パターンテーブル
        cur.execute(sql_create_pattern)
        #シーケンスデータテーブル
        cur.execute(sql_create_data)
        #コミット
        conn.commit()

def insert_mst():
    # CSV 読込
    # headerあり(なしの場合None)
    df = pd.read_csv(filepath_data_csv, header=0, names=['name', 'sequence_data'])
    print(df)
    # DBをに接続
    with sqlite3.connect(filepath_db) as conn:
        # DBにデータを挿入（データを置換）
        df.to_sql("M_SEQUENCE", con=conn, if_exists='replace'), 
        print(df)

def insert_data():
    # CSV 読込
    # headerあり(なしの場合None)
    df = pd.read_csv(filepath_primer_csv, header=0, names=['primer', 'groupname', 'pattern', 'length', 'GC', 'Tm', 'stockConcentration', 'stockLocation', 'memo'])
    print(df)
    # DBをに接続
    with sqlite3.connect(filepath_db) as conn:
        # DBにデータを挿入（データを置換）
        df.to_sql("M_PATTERN", con=conn, if_exists='replace')

def execute():
    with sqlite3.connect(filepath_db) as conn:
        # SQLを実行し、結果を取得
        df = pd.read_sql_query(sql_select_execute,conn)
        print(df)
        # CSVに保存する
        # INDEXを出力しない
        df.to_csv(filepath_result_csv, index=False, header=['primer', 'groupname', 'pattern', 'length', 'GC', 'Tm', 'stockConcentration', 'stockLocation' ,'memo' , 'primer', 'sequence_name'])
        
if __name__ == '__main__':
    # テーブル作成
    create_table()
    # マスタ挿入
    insert_mst()
    # データ挿入
    insert_data()
    # 変換処理＆出力
    execute()