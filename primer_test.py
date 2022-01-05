# coding: UTF-8
import sqlite3
import pandas as pd

#シーケンスデータファイル
# filepath_data_csv = r"C:\Users\sh527\Downloads\a5m2_2.16.2_x64\data.csv"
filepath_data_csv = r"data.csv"
#プライマーファイル
# filepath_primer_csv = r"C:\Users\sh527\Downloads\a5m2_2.16.2_x64\primer.csv"
filepath_primer_csv = r"primer.csv"
#結果ファイル名
# filepath_result_csv = r"C:\Users\sh527\Downloads\a5m2_2.16.2_x64\result.csv"
filepath_result_csv = r"result.csv"

#作業DB
filepath_db = r"test.db"
#一時テーブル
sql_create_tmp = "CREATE TABLE IF NOT EXISTS M_PATTERN( primer name text primary key , groupname text , pattern text , length INTEGER , GC REAL , Tm , stockConcentration , stockLocation , memo)"
#マスタテーブル
sql_create_mst = "CREATE TABLE IF NOT EXISTS M_SEQUENCE(name text primary key, sequence_data text)"
#結果テーブル
sql_create_result = "CREATE TABLE IF NOT EXISTS M_TEST(primer text primary key, sequence_name text)"

#SQL
sql_select_execute ='''

SELECT
       t1.primer
       ,group_concat(t2.name, ',') 
  FROM
       m_pattern t1
     , m_sequence t2 
 WHERE
       t2.sequence_data LIKE '%'|| t1.pattern || '%'
GROUP BY
       t1.primer 
'''
def create_table():
    #DBをに接続してテーブルを作成
    with sqlite3.connect(filepath_db) as conn:
        cur = conn.cursor()
        #一時テーブル
        cur.execute(sql_create_tmp)
        #マスタテーブル
        cur.execute(sql_create_mst)
        #結果テーブル
        cur.execute(sql_create_result)
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
        # test = pd.read_sql_query(sql_delete_m_test,conn)
        # print(test)
        df = pd.read_sql_query(sql_select_execute,conn)
        print(df)
        # CSVに保存する
        # INDEXを出力しない
        df.to_csv(filepath_result_csv, index=False, header=['primer', 'sequence_name'])
        
if __name__ == '__main__':
    # テーブル作成
    create_table()
    # マスタ挿入
    insert_mst()
    # データ挿入
    insert_data()
    # 変換処理＆出力
    execute()
