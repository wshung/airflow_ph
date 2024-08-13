import pandas as pd
import numpy as np
from utils.db_conn_info import host_name, host_name_t, database_name, port, user_name, user_pwd
from utils.encryption import TagProjectEncryption
from sqlalchemy import create_engine
import psycopg2


def get_YN(code):
    if code == 1:
        return '是'
    elif code == 0:
        return '否'
    else:
        return None

def get_YNO(code):
    if code == 1:
        return '是'
    elif code == 0:
        return '否'
    elif code == -1:
        return '舊'
    else:
        return None            

def get_years(days):
    try:
        return round(days.days/365.25, 2)
    except:
        return days
              
def get_gender(code):
    if code == 1:
        return '男性'
    elif code == 2:
        return '女性'
    else:
        return None

def get_caryears(days):
    try:
        return round(days.days/365.25, 2)
    except:
        return days

def get_regist(code):
    if code == 1:
        return '線下'
    elif code == 2:
        return '線上'
    else:
        return None

def get_oppstatus(code):
    if code == 'D':
        return '已戰敗或在排除已下訂條件後，三年內未修改資料記錄'
    elif code == 'C':
        return '最近建立有望客時間小於或等於訂單成立時間。'
    elif code == 'N':
        return '未定義'
    elif code == 'Y':
        return '歸戶表且有望客表有的資料'
    else:
        return None

def to_tag_table(df, category_name):
    ''' Stack tags and values by index'''
    tag_table = df.stack(dropna=True).reset_index(name='value')
    tag_table.rename(columns = { 'level_2': 'tag_name', 'value': 'tag_value'}, inplace = True)
    tag_table['bcat'] = category_name
    tag_table['mcat'] = tag_table['tag_name'].transform(lambda x: x.split('_')[0])
    tag_table['scat'] = tag_table['tag_name'].transform(lambda x: x.split('_')[1])
    return tag_table.drop_duplicates()

def get_age_range(age):
    bins = [-np.inf, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, np.inf]
    names = ['20以下', '21-25', '26-30', '31-35', '36-40', '41-45', '46-50', '51-55', '56-60', '61-65', '65以上']
    return pd.cut(age, bins, labels=names)

def change_language(dayname):
    if pd.isna(dayname):
        return '未知'
    else:
        if  dayname == 'Monday':
            return '星期一'
        elif dayname == 'Tuesday':
            return '星期二'
        elif dayname == 'Wednesday' :
            return '星期三'
        elif dayname == 'Thursday' :
            return '星期四'
        elif dayname == 'Friday' :
            return '星期五'
        elif dayname == 'Saturday' :
            return '星期六'
        elif dayname == 'Sunday' :
            return '星期日'


def delete_old_tag(table_name: pd.DataFrame, prefix_list: list):
    conn = psycopg2.connect(
        database = database_name,
        user = user_name,
        password = user_pwd,
        host = host_name,
        port = port
    )
    delete_sql = '''
    DELETE FROM dt_tag.{}
    where LEFT(tag_id, 6) in {};
    '''.format(table_name, [i for i in prefix_list]).replace('[','(').replace( ']', ')')
    print(delete_sql)

    try:
        cur = conn.cursor()
        cur.execute(delete_sql)
        conn.commit()
        cur.close()
        conn.close()
        return 'Done'
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    

def write_into_tag_table(df:pd.DataFrame, table_name: str, schema_name: str, method:str = 'replace'):
    if method not in ['replace', 'keep']:
        return "method must be one of choise in ['replace', 'keep']."
    if isinstance(df, pd.DataFrame):
        if method == 'replace':
            try:
                list_of_tag_id_prefix = df['tag_id'].transform(lambda x: x[:6]).drop_duplicates().tolist()
                print('Try to delete old data in {}...'.format(list_of_tag_id_prefix))
            except Exception as error:
                return ("An exception occurred:", error) 
            try:
                delete_old_tag(table_name = table_name, prefix_list = list_of_tag_id_prefix)
                print('Success!')
            except Exception as error:
                return ("An exception occurred:", error) 

        print('Try to write new data...')
        encry_mgr = TagProjectEncryption(b'ProjectInfo')
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(user_name, encry_mgr.decrypt_text(user_pwd), host_name_t, port, database_name))
        try:
            msg = df.to_sql(name = table_name, schema = schema_name, con = engine, index = False, if_exists = 'append', method=None)
            return 'Success! {} rows'.format(msg)
        except Exception as error:
                return ("An exception occurred:", error)
    else:
        return "'df' must be pd.DataFrame."
    


# 5.會員貢獻度 - 區分成零貢獻、低貢獻、中貢獻和高貢獻，4類
def get_customer_contribution(amount, lowv, medv):
    bins = [-1, 0, lowv, medv, np.inf]
    names = ['零貢獻', '低貢獻', '中貢獻', '高貢獻']
    return pd.cut(amount, bins, labels=names)
# amount:欲分析之金額欄位
# medv: 中貢獻金額
# lowv:低貢獻金額


# 5.會員忠誠度
def get_customer_loyalty(df, amount, times):
    df['amount_q'] = pd.qcut(df[amount], 2, labels = False) # 交易金額以四分位(Q1、Q2和 Q3)作切分標準
    df['times_q'] = pd.qcut(df[times], 4, labels = False, duplicates='drop') # 交易次數以四分位(Q1、Q2和 Q3)作切分標準
    df['loyalty'] = np.nan
    for idx, item in df.iterrows():
        a = item['amount_q']
        t = item['times_q']

        if (a == 0) & (t == 0):
            df.loc[idx, 'loyalty'] = 1
        elif (a == 1) & (t == 0):
            df.loc[idx, 'loyalty'] = 2
        elif (a == 0) & (t >= 1):
            df.loc[idx, 'loyalty'] = 3
        elif (a == 1) & (t == 1) | (t == 2):
            df.loc[idx, 'loyalty'] = 4
        elif (a == 1) & (t == 3):
            df.loc[idx, 'loyalty'] = 5
    return df['loyalty']

# df:欲分析之包含交易金額和交易頻率之資料表
# amount: 欲分析之金額欄位
# times:欲分析之交易頻率欄位



# 7. 消費金額區間
def get_amount_range(amount, intval, upper, minintval_star ,minintval, minupper):
    # bins = [0, 3000, 5000, np.inf]
    # names = ['3k以下', '3k~5k', '5k以上']
    bins = [i for i in range(0, upper+1, intval)] + [np.inf]
    names = [minintval_star +'k以下'] + ['{}k~{}k'.format(i, i+minintval)  for i in range(minintval_star, minupper, minintval)] + ['50k以上']
    return pd.cut(amount, bins, labels=names)

# amount:欲分析之金額欄位
# intval:多少元切一個區間。每5000切一個區間，請填5000
# upper: 金額上限
# minintval_star:表示最低區間的名稱。5k以下，請填5
# minintval:表示區間間隔名稱。每5000為一區間(5k~10k)，請填5
# minupper:表示最高區間名稱。50k，請填50