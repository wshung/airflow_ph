#!/usr/bin/env python

import numpy as np
import pandas as pd
import psycopg2
import warnings
warnings.filterwarnings('ignore')
from datetime import date
from datetime import datetime
import openpyxl
today = date.today()
from utils.helpers import get_YN, get_YNO, delete_old_tag, write_into_tag_table



def doing_query(query_string: str)-> pd.DataFrame:
    from utils.db_conn_info import host_name, database_name, port, user_name, user_pwd
    from utils.encryption import TagProjectEncryption

    encry_mgr = TagProjectEncryption(b'ProjectInfo')

    print(query_string)
    conn = psycopg2.connect(
        database = database_name,
        user = user_name,
        password = encry_mgr.decrypt_text(user_pwd),
        host = host_name,
        port = port
    )
    result = pd.read_sql(query_string, conn)
    conn.close()
    return result





# 抓人表 no these- lxg_id, email, mobile,
query_string = """
SELECT yl_id,
sso_id,
pii_token,
first_opp_time,
last_opp_time,
first_test_drive_time,
last_test_drive_time,
first_buy_time,
last_buy_time,
first_deliver_time,
last_deliver_time
FROM info.cmain_lxg_people
;
"""
cmain_lxg_people = doing_query(query_string)


# phone_to_id
query_string = """
select 
pii_token,phone, convert_from(decrypt(decode(phone, 'base64'),'evm$9xyyBar%8ZpRSMNdQEgwZrdY8qFx','aes-ecb'), 'UTF8') AS de_columnname
from
info.phone_to_id
;
"""
cmain_lxg_pii_de = doing_query(query_string)


# 抓經緯度表(目前只有車主)
# query_string = """
# SELECT master_mobile,
# latitude,
# longitude
# FROM xdt_lxg.z_lxg_owner_address_latlon
# ;
# """
# z_lxg_latlon = doing_query(query_string)

# 抓有望客表
query_string = """
SELECT cst_mphone,
incomelevel,
marriagestatus,
opp_grade
FROM xdt_lxg.lxg_nv_opp_cst
;
"""
lxg_nv_opp_cst = doing_query(query_string)

# 抓車籍表
query_string = """
SELECT ownermobile_09,
regono,
insuranceexpiry,
warrantyexpiry,
deliverydate,
bonus, 
bonusexpiry,
modelcode,
extcolour,
intcolour
FROM xdt_lxg.lxg_nv_vehicle
;
"""
lxg_nv_vehicle = doing_query(query_string)

# 抓工單
query_string = """
SELECT dlr, 
dlrcode,
dept,
deptcode,
regono,
rono,
trandate,
odoreading,
status,
totalsell
FROM xdt_lxg.lxg_nv_svcro
where trandate >= now() - interval '365 day'
;
"""
lxg_nv_svcro = doing_query(query_string)

# 抓訂單
query_string = """
SELECT dlr, 
dlr_code,
dept,
dept_code,
rego_no,
rbo_no,
sts_desc,
order_date,
totalamt,
pay_method,
model_code
FROM xdt_lxg.lxg_nv_veh_rbo
where order_date >= now() - interval '365 day'
;
"""
lxg_nv_veh_rbo = doing_query(query_string)

# 抓CRM
# query_string = """
# SELECT *
# FROM xdt_lxg.lxg_nv_crm
# where opendate >= now() - interval '365 day'
# ;
# """
# lxg_nv_crm = doing_query(query_string)

# # 抓試駕
# query_string = """
# SELECT *
# FROM xdt_lxg.lxg_nv_testdrive
# where testdrivedate >= now() - interval '365 day'
# ;
# """
# lxg_nv_testdrive = doing_query(query_string)

# # web_member
# query_string = """
# SELECT line_uid,
# phone,
# pk,
# uuid,
# birthday, 
# city
# FROM xdt_lxg.lxg_web_member
# ;
# """
# lxg_web_member = doing_query(query_string)

# nweb_user_member_info
query_string = """
SELECT memid,
phone_09,
birthday, 
city
FROM xdt_lxg.lxg_nweb_user_member_infos
;
"""
lxg_nweb_user_member_infos = doing_query(query_string)

# nweb_user_members
query_string = """
SELECT memid,
status,
luxgenuserid,
lineuserid
FROM xdt_lxg.lxg_nweb_user_members
;
"""
lxg_nweb_user_members = doing_query(query_string)

# # lxg_web_reservation
# query_string = """
# SELECT *
# FROM xdt_lxg.lxg_web_reservation
# where bookdate >= now() - interval '365 day'
# ;
# """
# lxg_web_reservation = doing_query(query_string)

# lxg_nv_dept
query_string = """
SELECT dealercode,
deptcode,
dept,
contactzipcode
FROM xdt_lxg.lxg_nv_dept
;
"""
lxg_nv_dept = doing_query(query_string)

# z_lxg_car_smodel(整理車系)

z_lxg_car_smodel = pd.read_excel('/home/eileen_liao_yulon_group_com/airflow/dags/luxgen/納智捷車系對照.xlsx' )

# z_ylg_postcode

z_ylg_postcode = pd.read_excel('/home/eileen_liao_yulon_group_com/airflow/dags/luxgen/z_ylg_postcode.xlsx' )

# brand_tag_define
query_string = """
SELECT *
FROM dt_tag.brand_tag_define
;
"""
brand_tag_define = doing_query(query_string)




cmain_lxg_pii_de.rename(columns={'phone': 'mobile'}, inplace=True)
cmain_lxg_people = cmain_lxg_people.merge(cmain_lxg_pii_de, left_on='pii_token', right_on='pii_token',how='left')




lxg_nweb_user_members.rename(columns={'luxgenuserid': 'uuid', 'lineuserid': 'line_uid', }, inplace=True)
lxg_nweb_user_members = lxg_nweb_user_members.merge(lxg_nweb_user_member_infos, left_on='memid', right_on='memid',how='left')
cmain_lxg_pii_de_phone = cmain_lxg_pii_de[['mobile','de_columnname']]
lxg_nweb_user_members = lxg_nweb_user_members.merge(cmain_lxg_pii_de_phone, left_on='phone_09', right_on='mobile',how='left')




cmain_lxg_people_lxgid = cmain_lxg_people[['yl_id', 'sso_id' ,'mobile']]
lxg_nweb_user_members_memid = lxg_nweb_user_members[['memid' ,'mobile']]
cmain_lxg_people_lxgid = cmain_lxg_people_lxgid.merge(lxg_nweb_user_members_memid, left_on='mobile', right_on='mobile',how='left')


# ## 消費紀錄



cmain_lxg_people_id = cmain_lxg_people[['yl_id', 'sso_id' ,'mobile']]


# ### 訂單資料

# #### 近1年 & 近三個月的消費金額和次數




# 篩有效的訂單
lxg_nv_veh_rbo_eft = lxg_nv_veh_rbo[lxg_nv_veh_rbo['sts_desc'].isin(['受訂',  '結帳', '配車',  '領牌', '交車核准', '交車'])]

# 計算訂單日期和資料更新日期的差幾天(days)
lxg_nv_veh_rbo_eft['day_diff'] =  (today - pd.to_datetime(lxg_nv_veh_rbo_eft['order_date'], errors='coerce').dt.date)
# lxg_nv_veh_rbo_eft['day_diff'] =  lxg_nv_veh_rbo_eft['day_diff'].dt.days
lxg_nv_veh_rbo_eft['day_diff'] =  lxg_nv_veh_rbo_eft['day_diff'].apply(lambda x: x.days)
# 篩1年內的訂單
lxg_nv_veh_rbo_eft_oneyear = lxg_nv_veh_rbo_eft[(lxg_nv_veh_rbo_eft['day_diff'] >= 0 ) & (lxg_nv_veh_rbo_eft['day_diff'] <=365)]
lxg_nv_veh_rbo_eft_oneyear['cs_freq_1y'] = 1
lxg_nv_veh_rbo_eft_oneyear['cost_1y'] = lxg_nv_veh_rbo_eft_oneyear['totalamt']
lxg_nv_veh_rbo_eft_ctb = lxg_nv_veh_rbo_eft_oneyear[["rego_no", "cs_freq_1y", "cost_1y"]]

# 篩3個月內的訂單
lxg_nv_veh_rbo_eft_3m = lxg_nv_veh_rbo_eft[(lxg_nv_veh_rbo_eft['day_diff'] >= 0 ) & (lxg_nv_veh_rbo_eft['day_diff'] <=90)]
lxg_nv_veh_rbo_eft_3m['cs_freq_3m'] = 1
lxg_nv_veh_rbo_eft_3m['cost_3m'] = lxg_nv_veh_rbo_eft_3m['totalamt']
lxg_nv_veh_rbo_eft_ctb_3m = lxg_nv_veh_rbo_eft_3m[["rego_no", "cs_freq_3m", "cost_3m"]]


# ##### 最近1年訂單資料細項



# create new column for concat
lxg_nv_veh_rbo_eft_oneyear['cs_brand'] = '納智捷'
lxg_nv_veh_rbo_eft_oneyear['cs_type'] = '新車'
lxg_nv_veh_rbo_eft_oneyear['cs_cost'] = lxg_nv_veh_rbo_eft_oneyear['totalamt']
lxg_nv_veh_rbo_eft_oneyear['cs_area_dlrcode'] = lxg_nv_veh_rbo_eft_oneyear['dlr_code']
lxg_nv_veh_rbo_eft_oneyear['cs_area_deptcode'] = lxg_nv_veh_rbo_eft_oneyear['dept_code']
lxg_nv_veh_rbo_eft_oneyear['cs_payway'] = lxg_nv_veh_rbo_eft_oneyear['pay_method']
lxg_nv_veh_rbo_eft_oneyear['cs_carmodle'] = lxg_nv_veh_rbo_eft_oneyear['model_code']
lxg_nv_veh_rbo_eft_oneyear['cs_tradedate'] = lxg_nv_veh_rbo_eft_oneyear['order_date']
lxg_nv_veh_rbo_eft_oneyear['regono'] = lxg_nv_veh_rbo_eft_oneyear['rego_no']
veh_rbo_oneyear= lxg_nv_veh_rbo_eft_oneyear[['regono', 'cs_tradedate', 'day_diff', 'cs_brand', 'cs_type', 'cs_cost', 'cs_area_dlrcode', 'cs_area_deptcode', 'cs_payway', 'cs_carmodle' ]]


# ### 工單資料

# #### 最近1年&近三個月的消費金額和次數



# 篩有效的保修單 
lxg_nv_svcro_eft = lxg_nv_svcro[lxg_nv_svcro['status'].isin(['結帳'])]
lxg_nv_svcro_eft['rono_10'] = lxg_nv_svcro_eft['rono'].str.slice(0, 10)

# 計算保修單日期和資料更新日期的差幾天(days)
lxg_nv_svcro_eft['day_diff'] =  (today - pd.to_datetime(lxg_nv_svcro_eft['trandate'], errors='coerce').dt.date)
# lxg_nv_svcro_eft['day_diff'] =  lxg_nv_svcro_eft['day_diff'].dt.days
lxg_nv_svcro_eft['day_diff'] =  lxg_nv_svcro_eft['day_diff'].apply(lambda x: x.days)

# 篩1年內保修單
lxg_nv_svcro_eft_oneyear = lxg_nv_svcro_eft[(lxg_nv_svcro_eft['day_diff'] >= 0) & (lxg_nv_svcro_eft['day_diff'] <= 365)]
# 計算每輛車1年內總保修金額
lxg_nv_svcro_eft_oneyear_tsell = lxg_nv_svcro_eft_oneyear.groupby('regono')['totalsell'].sum().reset_index(name="cost_1y")
# 計算每輛車1年內回廠次數
# -先去除同廠同工單編號
lxg_nv_svcro_eft_oneyear_tfreq_tem = lxg_nv_svcro_eft_oneyear.drop_duplicates(['dlrcode', 'deptcode', 'regono','rono_10'])
# 計算回廠次數
lxg_nv_svcro_eft_oneyear_tfreq = lxg_nv_svcro_eft_oneyear_tfreq_tem.groupby('regono')['regono'].count().reset_index(name="cs_freq_1y")

# 篩3個月內保修單
lxg_nv_svcro_eft_3m = lxg_nv_svcro_eft[(lxg_nv_svcro_eft['day_diff'] >= 0) & (lxg_nv_svcro_eft['day_diff'] <= 90)]
# 計算每輛車3個月內總保修金額
lxg_nv_svcro_eft_3m_tsell = lxg_nv_svcro_eft_3m.groupby('regono')['totalsell'].sum().reset_index(name="cost_3m")
# 計算每輛車3個月內回廠次數
# -先去除同廠同工單編號
lxg_nv_svcro_eft_3m_tfreq_tem = lxg_nv_svcro_eft_3m.drop_duplicates(['dlrcode', 'deptcode', 'regono','rono_10'])
# 計算3個月內回廠次數
lxg_nv_svcro_eft_3m_tfreq = lxg_nv_svcro_eft_3m_tfreq_tem.groupby('regono')['regono'].count().reset_index(name="cs_freq_3m")


# ##### 最近1次消費金額



lxg_nv_svcro_eft['trandate'] = pd.to_datetime(lxg_nv_svcro_eft.trandate, format='%Y-%m-%d')
lxg_nv_svcro_eft['tf_trandate'] = lxg_nv_svcro_eft['trandate']
lxg_nv_svcro_eft_latest = lxg_nv_svcro_eft.loc[lxg_nv_svcro_eft.groupby('regono')['trandate'].idxmax()]
lxg_nv_svcro_eft_latest_date = lxg_nv_svcro_eft_latest[["regono", "trandate", "tf_trandate", "rono_10", "dlrcode","deptcode"]]
lxg_nv_svcro_eft_latest_date.rename(columns = {'tf_trandate':'maxtf_trandate', 'trandate':'max_trandate'}, inplace = True)
lxg_nv_svcro_eft_latest_date_cb = lxg_nv_svcro_eft_latest_date[["regono", "maxtf_trandate", "max_trandate"]].copy()

lxg_nv_svcro_eft_latest_tsell_tem = pd.merge(lxg_nv_svcro_eft, lxg_nv_svcro_eft_latest_date,  how='left', left_on=['dlrcode','deptcode','regono', 'rono_10'], right_on = ['dlrcode','deptcode','regono', 'rono_10'])

lxg_nv_svcro_eft_latest_tsell_tem1 = lxg_nv_svcro_eft_latest_tsell_tem[lxg_nv_svcro_eft_latest_tsell_tem['tf_trandate'] == lxg_nv_svcro_eft_latest_tsell_tem['maxtf_trandate']]
lxg_nv_svcro_eft_latest_tsell_tem2 = lxg_nv_svcro_eft_latest_tsell_tem1.groupby(['regono', 'rono_10'])['totalsell'].sum().reset_index(name="totalsell_latest")
lxg_nv_svcro_eft_latest_tsell = lxg_nv_svcro_eft_latest_tsell_tem2[["regono", "totalsell_latest"]]


# #### 最近1年工單資料細項
lxg_nv_svcro_eft_oneyear1 = lxg_nv_svcro_eft_oneyear.merge(lxg_nv_svcro_eft_latest_tsell, left_on='regono', right_on='regono', how='left')

lxg_nv_vehicle_tem = lxg_nv_vehicle[["regono", "modelcode"]]
lxg_nv_svcro_eft_oneyear_tem = lxg_nv_svcro_eft_oneyear1.merge(lxg_nv_vehicle_tem, left_on='regono', right_on='regono', how='left')

lxg_nv_svcro_eft_oneyear_tem['cs_brand'] = '納智捷'
lxg_nv_svcro_eft_oneyear_tem['cs_type'] = '售服'
lxg_nv_svcro_eft_oneyear_tem['cs_cost'] = lxg_nv_svcro_eft_oneyear_tem['totalsell_latest']
lxg_nv_svcro_eft_oneyear_tem['cs_area_dlrcode'] = lxg_nv_svcro_eft_oneyear_tem['dlrcode']
lxg_nv_svcro_eft_oneyear_tem['cs_area_deptcode'] = lxg_nv_svcro_eft_oneyear_tem['deptcode']
lxg_nv_svcro_eft_oneyear_tem['cs_payway'] = '未知'
lxg_nv_svcro_eft_oneyear_tem['cs_carmodle'] = lxg_nv_svcro_eft_oneyear_tem['modelcode']
lxg_nv_svcro_eft_oneyear_tem['cs_tradedate'] = lxg_nv_svcro_eft_oneyear_tem['trandate']
veh_svcro_oneyear= lxg_nv_svcro_eft_oneyear_tem[['regono', 'cs_tradedate', 'day_diff', 'cs_brand', 'cs_type', 'cs_cost' ,'cs_area_dlrcode', 'cs_area_deptcode', 'cs_payway', 'cs_carmodle' ]]


# #### join 近3個月消費金額和次數



# 車籍表join訂單
lxg_nv_vehicle_tem = lxg_nv_vehicle[["regono", "ownermobile_09"]]
lxg_rbo_ctb_3m = lxg_nv_vehicle_tem.merge(lxg_nv_veh_rbo_eft_ctb_3m, left_on='regono', right_on='rego_no', how='left')
lxg_rbo_ctb_3m=lxg_rbo_ctb_3m.drop("rego_no", axis=1)

# 車籍表join工單
lxg_svcro_ctb1_3m = lxg_nv_vehicle_tem.merge(lxg_nv_svcro_eft_3m_tsell, left_on='regono', right_on='regono', how='left')
lxg_svcro_ctb_3m = lxg_svcro_ctb1_3m.merge(lxg_nv_svcro_eft_3m_tfreq, left_on='regono', right_on='regono',how='left')

#  人表 join 訂單
# 未剔除法人和二手車商
cmain_lxg_people_rbo_ctb_3m = cmain_lxg_people.merge(lxg_rbo_ctb_3m, left_on='mobile', right_on='ownermobile_09',how='left')
# cmain_lxg_people_ctb.rename(columns = {'day_diff_x':'day_diff_order', 'day_diff_y':'day_diff_svcro'}, inplace = True)
cmain_lxg_people_rbo_ctb_3m['source'] = 'order'

#  人表 join 工單
# 未剔除法人和二手車商
cmain_lxg_people_svcro_ctb_3m = cmain_lxg_people.merge(lxg_svcro_ctb_3m, left_on='mobile', right_on='ownermobile_09',how='left')
# cmain_lxg_people_ctb.rename(columns = {'day_diff_x':'day_diff_order', 'day_diff_y':'day_diff_svcro'}, inplace = True)
cmain_lxg_people_svcro_ctb_3m['source'] = 'svcro'

# concat 訂單
consumption_pricefre_3m = pd.concat([cmain_lxg_people_rbo_ctb_3m, cmain_lxg_people_svcro_ctb_3m], ignore_index=True)
people_consumption_3m_price = consumption_pricefre_3m.groupby('mobile')['cost_3m'].sum().reset_index(name="total_cost_3m")
people_consumption_3m_freq = consumption_pricefre_3m.groupby('mobile')['cs_freq_3m'].sum().reset_index(name="total_freq_3m")
# 剔除法人和二手車商
# cmain_lxg_people_nooffical = cmain_lxg_people[cmain_lxg_people["gender"] != '未知']
# cmain_lxg_people_ctb = cmain_lxg_people_nooffical.merge(lxg_nv_veh_rbo_ctb8, left_on='mobile', right_on='ownermobile',how='left')


# #### Join 最近1年消費金額和次數

# 車籍表join訂單
lxg_nv_vehicle_tem = lxg_nv_vehicle[["regono", "ownermobile_09"]]
lxg_rbo_ctb = lxg_nv_vehicle_tem.merge(lxg_nv_veh_rbo_eft_ctb, left_on='regono', right_on='rego_no', how='left')
lxg_rbo_ctb=lxg_rbo_ctb.drop("rego_no", axis=1)
# 車籍表join工單
lxg_svcro_ctb1 = lxg_nv_vehicle_tem.merge(lxg_nv_svcro_eft_oneyear_tsell, left_on='regono', right_on='regono', how='left')
lxg_svcro_ctb = lxg_svcro_ctb1.merge(lxg_nv_svcro_eft_oneyear_tfreq, left_on='regono', right_on='regono',how='left')

#  人表 join 訂單
# 未剔除法人和二手車商
cmain_lxg_people_rbo_ctb = cmain_lxg_people.merge(lxg_rbo_ctb, left_on='mobile', right_on='ownermobile_09',how='left')
# cmain_lxg_people_ctb.rename(columns = {'day_diff_x':'day_diff_order', 'day_diff_y':'day_diff_svcro'}, inplace = True)
cmain_lxg_people_rbo_ctb['source'] = 'order'
#  人表 join 工單
# 未剔除法人和二手車商
cmain_lxg_people_svcro_ctb = cmain_lxg_people.merge(lxg_svcro_ctb, left_on='mobile', right_on='ownermobile_09',how='left')
# cmain_lxg_people_ctb.rename(columns = {'day_diff_x':'day_diff_order', 'day_diff_y':'day_diff_svcro'}, inplace = True)
cmain_lxg_people_svcro_ctb['source'] = 'svcro'

# concat 訂單
consumption_pricefre_oneyear = pd.concat([cmain_lxg_people_rbo_ctb, cmain_lxg_people_svcro_ctb], ignore_index=True)
people_consumption_oneyear_price = consumption_pricefre_oneyear.groupby('mobile')['cost_1y'].sum().reset_index(name="total_cost_1y")
people_consumption_oneyear_freq = consumption_pricefre_oneyear.groupby('mobile')['cs_freq_1y'].sum().reset_index(name="total_freq_1y")
# 剔除法人和二手車商
# cmain_lxg_people_nooffical = cmain_lxg_people[cmain_lxg_people["gender"] != '未知']
# cmain_lxg_people_ctb = cmain_lxg_people_nooffical.merge(lxg_nv_veh_rbo_ctb8, left_on='mobile', right_on='ownermobile',how='left')


# #### 合併最近1年訂單和工單細項
consumption_oneyear = pd.concat([veh_rbo_oneyear, veh_svcro_oneyear], ignore_index=True)

# # join 車籍表 & 訂單
# lxg_nv_veh_rbo_ctb1 = lxg_nv_vehicle.merge(lxg_nv_veh_rbo_eft_ctb, left_on='regono', right_on='rego_no', how='left')
# lxg_nv_veh_rbo_ctb2 = lxg_nv_veh_rbo_ctb1.merge(lxg_nv_svcro_eft_twoyear, left_on='regono', right_on='regono', how='left')
# lxg_nv_veh_rbo_ctb3 = lxg_nv_veh_rbo_ctb2.merge(lxg_nv_svcro_eft_latest_date_cb, left_on='regono', right_on='regono', how='left')
# lxg_nv_veh_rbo_ctb4 = lxg_nv_veh_rbo_ctb3.merge(lxg_nv_svcro_eft_latest_tsell, left_on='regono', right_on='regono',how='left')
# lxg_nv_veh_rbo_ctb5 = lxg_nv_veh_rbo_ctb4.merge(lxg_nv_svcro_eft_oneyear_tsell, left_on='regono', right_on='regono',how='left')
# lxg_nv_veh_rbo_ctb6 = lxg_nv_veh_rbo_ctb5.merge(lxg_nv_svcro_eft_oneyear_tfreq, left_on='regono', right_on='regono',how='left')
# lxg_nv_veh_rbo_ctb7 = lxg_nv_veh_rbo_ctb6.merge(lxg_nv_svcro_eft_3months_tsell, left_on='regono', right_on='regono',how='left')
# lxg_nv_veh_rbo_ctb8 = lxg_nv_veh_rbo_ctb7.merge(lxg_nv_svcro_eft_3months_tfreq, left_on='regono', right_on='regono',how='left')

# join 人表 
lxg_nv_vehicle_tem = lxg_nv_vehicle[["regono", "ownermobile_09", "extcolour" ,"intcolour"]]
consumption_oneyear_tem = lxg_nv_vehicle_tem.merge(consumption_oneyear, left_on='regono', right_on='regono', how='left')
# people_consumption_oneyear = cmain_lxg_people.merge(consumption_oneyear_tem, left_on='mobile', right_on='ownermobile',how='left')


consumption_oneyear=consumption_oneyear[consumption_oneyear.iloc[:, 2:].notnull().any(axis=1)]
consumption_oneyear = consumption_oneyear.drop_duplicates(keep='first')


lxg_nv_vehicle_tem=lxg_nv_vehicle_tem[lxg_nv_vehicle_tem.iloc[:, 3:].notnull().any(axis=1)]
lxg_nv_vehicle_tem = lxg_nv_vehicle_tem.drop_duplicates(keep='first')


# join 人表 
lxg_nv_vehicle_tem = lxg_nv_vehicle[["regono", "ownermobile_09", "extcolour" ,"intcolour"]]
consumption_oneyear_tem = lxg_nv_vehicle_tem.merge(consumption_oneyear, left_on='regono', right_on='regono', how='left')
people_consumption_oneyear = cmain_lxg_people_id.merge(consumption_oneyear_tem, left_on='mobile', right_on='ownermobile_09',how='left')


# #### 最近3個月訂單和工單細項



people_consumption_3m = people_consumption_oneyear[(people_consumption_oneyear['day_diff'] >= 0) & (people_consumption_oneyear['day_diff'] <= 90)]


# #### for最近1次資料標籤


# 篩最近1次的保修單
consumption_oneyear['cs_tradedate'] = pd.to_datetime(consumption_oneyear.cs_tradedate, format='%Y-%m-%d')
consumption_oneyear['cs_tradedate_tf'] = consumption_oneyear['cs_tradedate'].dt.date
consumption_oneyear['cs_tradedate_tf']  = pd.to_datetime(consumption_oneyear['cs_tradedate_tf'] )
consumption_latest = consumption_oneyear.loc[consumption_oneyear.groupby('regono')['cs_tradedate_tf'].idxmax()]

lxg_nv_vehicle_tem = lxg_nv_vehicle[["regono", "ownermobile_09", "extcolour" ,"intcolour"]]
consumption_latest_tem = lxg_nv_vehicle_tem.merge(consumption_latest, left_on='regono', right_on='regono', how='left')
people_consumption_latest = cmain_lxg_people_id.merge(consumption_latest_tem, left_on='mobile', right_on='ownermobile_09',how='left')


# ### 消費紀錄標籤表框架1

tran_id = cmain_lxg_people[['yl_id', 'mobile']]
tran_col = []
lxg_trantag = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)



tran_col = []
lxg_trantag_1 = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)



tran_col = []
lxg_trantag_2 = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)


tran_col = []
lxg_trantag_3 = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)




print(people_consumption_latest.head(100))
print(people_consumption_3m.head(100))
print(people_consumption_oneyear.head(100))


# ### 交易品牌

# 最近1次交易品牌
people_consumption_latest['cs_brand_f'] = np.where(people_consumption_latest['day_diff'] >= 0, '納智捷', None)
cs_brand_latest = people_consumption_latest[people_consumption_latest['cs_brand_f'].notnull()]
# 最近3個月交易品牌
people_consumption_3m['cs_brand_f_3m'] = np.where(people_consumption_3m['day_diff'] >= 0, '納智捷', None )
cs_brand_3m = people_consumption_3m[people_consumption_3m['cs_brand_f_3m'].notnull()]
# 最近1年交易品牌
people_consumption_oneyear['cs_brand_f_1y'] = np.where(people_consumption_oneyear['day_diff'] >= 0, '納智捷', None )
cs_brand_ly = people_consumption_oneyear[people_consumption_oneyear['cs_brand_f_1y'].notnull()]


lxg_trantag = lxg_trantag.join(cs_brand_latest.loc[:,["mobile", "cs_brand_f"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')

lxg_trantag = lxg_trantag.join(cs_brand_3m.loc[:,["mobile", "cs_brand_f_3m"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')

lxg_trantag = lxg_trantag.join(cs_brand_ly.loc[:,["mobile", "cs_brand_f_1y"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')


# #### 交易類型


# 最近1次交易類型
people_consumption_latest['cs_type_f'] = np.where(people_consumption_latest['day_diff'] >= 0, people_consumption_latest['cs_type'], None )
cs_type_latest = people_consumption_latest[people_consumption_latest['cs_type_f'].notnull()]
# 最近3個月交易類型
people_consumption_3m['cs_type_f_3m'] = np.where(people_consumption_3m['day_diff'] >= 0, people_consumption_3m['cs_type'], None )
cs_type_3m = people_consumption_3m[people_consumption_3m['cs_type_f_3m'].notnull()]
# 最近1年交易類型
people_consumption_oneyear['cs_type_f_1y'] = np.where(people_consumption_oneyear['day_diff'] >= 0, people_consumption_oneyear['cs_type'], None)
cs_type_ly = people_consumption_oneyear[people_consumption_oneyear['cs_type_f_1y'].notnull()]




lxg_trantag = lxg_trantag.join(cs_type_latest.loc[:,["mobile", "cs_type_f"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')
lxg_trantag = lxg_trantag.join(cs_type_3m.loc[:,["mobile", "cs_type_f_3m"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')
lxg_trantag = lxg_trantag.join(cs_type_ly.loc[:,["mobile", "cs_type_f_1y"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')


# #### 交易金額



# 最近1次交易金額
people_consumption_latest['cs_cost_f'] = np.where(people_consumption_latest['day_diff'] >= 0, people_consumption_latest['cs_cost'], None)
cs_cost_latest = people_consumption_latest[people_consumption_latest['cs_cost_f'].notnull()]
# 最近3個月交易金額
people_consumption_3m_price['cs_cost_f_3m'] = people_consumption_3m_price['total_cost_3m']
cs_cost_3m = people_consumption_3m_price[people_consumption_3m_price['cs_cost_f_3m'].notnull()]
#  最近1年交易金額
people_consumption_oneyear_price['cs_cost_f_1y'] = people_consumption_oneyear_price['total_cost_1y']
cs_cost_1y = people_consumption_oneyear_price[people_consumption_oneyear_price['cs_cost_f_1y'].notnull()]




lxg_trantag = lxg_trantag.join(cs_cost_latest.loc[:,["mobile", "cs_cost_f"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')
lxg_trantag = lxg_trantag.join(cs_cost_3m.loc[:,["mobile", "cs_cost_f_3m"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')
lxg_trantag = lxg_trantag.join(cs_cost_1y.loc[:,["mobile", "cs_cost_f_1y"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')


# #### 交易次數



# 最近3個月交易次數
people_consumption_3m_freq['cs_freq_f_3m'] = people_consumption_3m_freq['total_freq_3m']
cs_freq_3m = people_consumption_3m_freq[people_consumption_3m_freq['cs_freq_f_3m'].notnull()]
# 最近1年交易次數
people_consumption_oneyear_freq['cs_freq_f_1y'] = people_consumption_oneyear_freq['total_freq_1y']
cs_freq_1y = people_consumption_oneyear_freq[people_consumption_oneyear_freq['cs_freq_f_1y'].notnull()]




lxg_trantag = lxg_trantag.join(cs_freq_3m.loc[:,["mobile", "cs_freq_f_3m"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')
lxg_trantag = lxg_trantag.join(cs_freq_1y.loc[:,["mobile", "cs_freq_f_1y"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')


    # #### 消費能力


def get_cost_level(amount):
    if (amount <= Q1) | (np.isnan(amount)):
        return '低'
    elif (amount > Q1) | (amount <= Q3):
        return '中'
    elif (amount > Q3) :
        return '高'
    else:
        return None
            
# 最近1次
Q1 = people_consumption_latest[people_consumption_latest['cs_cost'].notna()]['cs_cost'].quantile(0.25).astype('int').item()
Q3 = people_consumption_latest[people_consumption_latest['cs_cost'].notna()]['cs_cost'].quantile(0.75).astype('int').item()
people_consumption_latest['cs_cost_level'] = people_consumption_latest['cs_cost'].transform(get_cost_level)
cs_cost_level_latest = people_consumption_latest[people_consumption_latest['cs_cost_level'].notnull()]
# 近3個月
Q1 = people_consumption_3m_price[people_consumption_3m_price['total_cost_3m'].notna()]['total_cost_3m'].quantile(0.25).astype('int').item()
Q3 = people_consumption_3m_price[people_consumption_3m_price['total_cost_3m'].notna()]['total_cost_3m'].quantile(0.75).astype('int').item()
people_consumption_3m_price['cs_cost_level_3m'] = people_consumption_3m_price['total_cost_3m'].transform(get_cost_level)
cs_cost_level_3m = people_consumption_3m_price[people_consumption_3m_price['cs_cost_level_3m'].notnull()]
# 最近1年
Q1 = people_consumption_oneyear_price[people_consumption_oneyear_price['total_cost_1y'].notna()]['total_cost_1y'].quantile(0.25).astype('int').item()
Q3 = people_consumption_oneyear_price[people_consumption_oneyear_price['total_cost_1y'].notna()]['total_cost_1y'].quantile(0.75).astype('int').item()
people_consumption_oneyear_price['cs_cost_level_1y'] = people_consumption_oneyear_price['total_cost_1y'].transform(get_cost_level)
cs_cost_level_1y = people_consumption_oneyear_price[people_consumption_oneyear_price['cs_cost_level_1y'].notnull()]



cs_cost_level_latest=cs_cost_level_latest[cs_cost_level_latest.iloc[:, 3:].notnull().any(axis=1)]
cs_cost_level_latest = cs_cost_level_latest.drop_duplicates(keep='first')

lxg_trantag = lxg_trantag.join(cs_cost_level_latest.loc[:,["mobile", "cs_cost_level"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')



lxg_trantag = lxg_trantag.join(cs_cost_level_3m.loc[:,["mobile", "cs_cost_level_3m"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')




lxg_trantag = lxg_trantag.join(cs_cost_level_1y.loc[:,["mobile", "cs_cost_level_1y"]].set_index('mobile'), on='mobile')
lxg_trantag=lxg_trantag[lxg_trantag.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag = lxg_trantag.drop_duplicates(keep='first')


# #### 交易區域&據點

# ##### 交易區域和據點轉化縣市



lxg_nv_dept_tem = lxg_nv_dept.copy()
lxg_nv_dept_tem['deal_dept'] = lxg_nv_dept_tem['dealercode']+lxg_nv_dept_tem['deptcode']
lxg_nv_dept_tem = lxg_nv_dept_tem[["deal_dept", "contactzipcode", "dept"]]

z_ylg_postcode_tem = z_ylg_postcode.copy()
z_ylg_postcode_tem =z_ylg_postcode_tem[['post_code', 'city' ,'area']]

cs_area_city = lxg_nv_dept_tem.merge(z_ylg_postcode_tem, left_on='contactzipcode', right_on='post_code',how='left')
cs_area_city = cs_area_city[["deal_dept", "contactzipcode", "dept", 'city', 'area']]




# 最近1次
people_consumption_latest['cs_area_code'] = np.where(people_consumption_latest['day_diff'] >= 0, people_consumption_latest['cs_area_dlrcode'] + people_consumption_latest['cs_area_deptcode'] , None)
cs_area_code_latest = people_consumption_latest[people_consumption_latest['cs_area_code'].notnull()]
cs_area_code_latest= cs_area_code_latest.drop_duplicates(['mobile','cs_area_code'], keep='first')
cs_area_code_latest = cs_area_code_latest.merge(cs_area_city, left_on='cs_area_code', right_on='deal_dept',how='left')
cs_area_code_latest = cs_area_code_latest.rename(columns = {'city':'city_latest', 'area':'area_latest','dept':'dept_latest' })

# 最近3個月
people_consumption_3m['cs_area_code_3m'] = np.where(people_consumption_3m['day_diff'] >= 0, people_consumption_3m['cs_area_dlrcode'] + people_consumption_3m['cs_area_deptcode'] , None)
cs_area_code_3m = people_consumption_3m[people_consumption_3m['cs_area_code_3m'].notnull()]
cs_area_code_3m= cs_area_code_3m.drop_duplicates(['mobile','cs_area_code_3m'], keep='first')
cs_area_code_3m = cs_area_code_3m.merge(cs_area_city, left_on='cs_area_code_3m', right_on='deal_dept',how='left')
cs_area_code_3m = cs_area_code_3m.rename(columns = {'city':'city_3m', 'area':'area_3m','dept':'dept_3m'  })
# 最近1年
people_consumption_oneyear['cs_area_code_1y'] = np.where(people_consumption_oneyear['day_diff'] >= 0, people_consumption_oneyear['cs_area_dlrcode'] + people_consumption_oneyear['cs_area_deptcode'] , None)
cs_area_code_1y = people_consumption_oneyear[people_consumption_oneyear['cs_area_code_1y'].notnull()]
cs_area_code_1y= cs_area_code_1y.drop_duplicates(['mobile','cs_area_code_1y'], keep='first')
cs_area_code_1y = cs_area_code_1y.merge(cs_area_city, left_on='cs_area_code_1y', right_on='deal_dept',how='left')
cs_area_code_1y = cs_area_code_1y.rename(columns = {'city':'city_1y', 'area':'area_1y','dept':'dept_1y' })



cs_area_code_latest=cs_area_code_latest[cs_area_code_latest.iloc[:, 3:].notnull().any(axis=1)]
cs_area_code_latest = cs_area_code_latest.drop_duplicates(keep='first')
lxg_trantag_1 = lxg_trantag_1.join(cs_area_code_latest.loc[:,["mobile", "city_latest", 'dept_latest' ]].set_index('mobile'), on='mobile')
lxg_trantag_1=lxg_trantag_1[lxg_trantag_1.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag_1 = lxg_trantag_1.drop_duplicates(keep='first')

cs_area_code_3m = cs_area_code_3m[cs_area_code_3m.iloc[:, 3:].notnull().any(axis=1)]
cs_area_code_3m = cs_area_code_3m.drop_duplicates(keep='first')
lxg_trantag_2 = lxg_trantag_2.join(cs_area_code_3m.loc[:,["mobile", "city_3m", 'dept_3m']].set_index('mobile'), on='mobile')
lxg_trantag_2=  lxg_trantag_2[lxg_trantag_2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag_2 = lxg_trantag_2.drop_duplicates(keep='first')

cs_area_code_1y = cs_area_code_1y[cs_area_code_1y.iloc[:, 3:].notnull().any(axis=1)]
cs_area_code_1y = cs_area_code_1y.drop_duplicates(keep='first')
lxg_trantag_3 = lxg_trantag_3.join(cs_area_code_1y.loc[:,["mobile", "city_1y", 'dept_1y']].set_index('mobile'), on='mobile')
lxg_trantag_3 = lxg_trantag_3[lxg_trantag_3.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag_3 = lxg_trantag_3.drop_duplicates(keep='first')


# #### 交易方式



# 最近1次
people_consumption_latest['cs_payway_f'] = np.where(people_consumption_latest['day_diff'] >= 0, people_consumption_latest['cs_payway'] , None )
cs_payway_latest = people_consumption_latest[people_consumption_latest['cs_payway_f'].notnull()]
cs_payway_latest= cs_payway_latest.drop_duplicates(['mobile','cs_payway_f'], keep='first')

# 最近3個月
people_consumption_3m['cs_payway_f_3m'] = np.where(people_consumption_3m['day_diff'] >= 0, people_consumption_3m['cs_payway'] , None )
cs_payway_3m = people_consumption_3m[people_consumption_3m['cs_payway_f_3m'].notnull()]
cs_payway_3m= cs_payway_3m.drop_duplicates(['mobile','cs_payway_f_3m'], keep='first')

# 最近1年
people_consumption_oneyear['cs_payway_f_1y'] = np.where(people_consumption_oneyear['day_diff'] >= 0, people_consumption_oneyear['cs_payway'], None)
cs_payway_1y = people_consumption_oneyear[people_consumption_oneyear['cs_payway_f_1y'].notnull()]
cs_payway_1y= cs_payway_1y.drop_duplicates(['mobile','cs_payway_f_1y'], keep='first')



lxg_trantag_1 = lxg_trantag_1.join(cs_payway_latest.loc[:,["mobile", "cs_payway_f"]].set_index('mobile'), on='mobile')
lxg_trantag_1 = lxg_trantag_1[lxg_trantag_1.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag_1 = lxg_trantag_1.drop_duplicates(keep='first')
lxg_trantag_1 = lxg_trantag_1.join(cs_payway_3m.loc[:,["mobile", "cs_payway_f_3m"]].set_index('mobile'), on='mobile')
lxg_trantag_1 = lxg_trantag_1[lxg_trantag_1.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag_1 = lxg_trantag_1.drop_duplicates(keep='first')
lxg_trantag_1 = lxg_trantag_1.join(cs_payway_1y.loc[:,["mobile", "cs_payway_f_1y"]].set_index('mobile'), on='mobile')
lxg_trantag_1 = lxg_trantag_1[lxg_trantag_1.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag_1 = lxg_trantag_1.drop_duplicates(keep='first')


# # tag_table 1



cstag_names_mapping1 = {
'yl_id' : 'yl_id',
'cs_brand_f': '最近一次交易_交易品牌',
'cs_brand_f_3m': '近三個月交易_交易品牌',
'cs_brand_f_1y': '近一年交易_交易品牌',
'cs_type_f': '最近一次交易_交易類型',
'cs_type_f_3m': '近三個月交易_交易類型',
'cs_type_f_1y': '近一年交易_交易類型',           
'cs_cost_f': '最近一次交易_交易金額',
'cs_cost_f_3m': '近三個月交易_交易金額',
'cs_cost_f_1y': '近一年交易_交易金額',
'cs_freq_f_3m': '近三個月交易_交易次數',
'cs_freq_f_1y': '近一年交易_交易次數',
'cs_cost_level': '最近一次交易_消費能力',
'cs_cost_level_3m': '近三個月交易_消費能力',
'cs_cost_level_1y': '近一年交易_消費能力',
}

tmp_table1 = lxg_trantag.loc[:, list(cstag_names_mapping1.keys())].copy()
tmp_table1.rename(columns = cstag_names_mapping1, inplace = True)


tag_table1 = tmp_table1.set_index('yl_id').stack().reset_index(name='value')
tag_table1.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table1['bcat'] = '消費紀錄'
tag_table1['mcat'] = tag_table1['tag_name'].transform(lambda x: x.split('_')[0])
tag_table1['scat'] = tag_table1['tag_name'].transform(lambda x: x.split('_')[1])
tag_table1 = tag_table1.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table1['crt_dt'] = datetime.now()
tag_table1['upd_dt'] = datetime.now()




cstag_names_mapping1_1 = {
'yl_id' : 'yl_id',
'city_latest': '最近一次交易_交易區域',
'dept_latest': '最近一次交易_交易據點',
'cs_payway_f': '最近一次交易_交易方式',
'cs_payway_f_3m': '近三個月交易_交易方式',
'cs_payway_f_1y': '近一年交易_交易方式'
}

tmp_table_1 = lxg_trantag_1.loc[:, list(cstag_names_mapping1_1.keys())].copy()
tmp_table_1.rename(columns = cstag_names_mapping1_1, inplace = True)




tag_table_1 = tmp_table_1.set_index('yl_id').stack().reset_index(name='value')
tag_table_1.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table_1['bcat'] = '消費紀錄'
tag_table_1['mcat'] = tag_table_1['tag_name'].transform(lambda x: x.split('_')[0])
tag_table_1['scat'] = tag_table_1['tag_name'].transform(lambda x: x.split('_')[1])
tag_table_1 = tag_table_1.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table_1['crt_dt'] = datetime.now()
tag_table_1['upd_dt'] = datetime.now()




cstag_names_mapping1_2 = {
'yl_id' : 'yl_id',
'city_3m': '近三個月交易_交易區域',
'dept_3m': '近三個月交易_交易據點'
}

tmp_table_2 = lxg_trantag_2.loc[:, list(cstag_names_mapping1_2.keys())].copy()
tmp_table_2.rename(columns = cstag_names_mapping1_2, inplace = True)




tag_table_2 = tmp_table_2.set_index('yl_id').stack().reset_index(name='value')
tag_table_2.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table_2['bcat'] = '消費紀錄'
tag_table_2['mcat'] = tag_table_2['tag_name'].transform(lambda x: x.split('_')[0])
tag_table_2['scat'] = tag_table_2['tag_name'].transform(lambda x: x.split('_')[1])
tag_table_2 = tag_table_2.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table_2['crt_dt'] = datetime.now()
tag_table_2['upd_dt'] = datetime.now()




tag_table_2 = tag_table_2.drop_duplicates(keep='first')




cstag_names_mapping1_3 = {
'yl_id' : 'yl_id',
'city_1y': '近一年交易_交易區域',
'dept_1y': '近一年交易_交易據點'
}

tmp_table_3 = lxg_trantag_3.loc[:, list(cstag_names_mapping1_3.keys())].copy()
tmp_table_3.rename(columns = cstag_names_mapping1_3, inplace = True)



tag_table_3 = tmp_table_3.set_index('yl_id').stack().reset_index(name='value')
tag_table_3.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table_3['bcat'] = '消費紀錄'
tag_table_3['mcat'] = tag_table_3['tag_name'].transform(lambda x: x.split('_')[0])
tag_table_3['scat'] = tag_table_3['tag_name'].transform(lambda x: x.split('_')[1])
tag_table_3 = tag_table_3.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table_3['crt_dt'] = datetime.now()
tag_table_3['upd_dt'] = datetime.now()


tag_table_3 = tag_table_3.drop_duplicates(keep='first')

tag_table1 = pd.concat([tag_table1, tag_table_1, tag_table_2 ,tag_table_3], ignore_index=True)

tag_table1 = tag_table1.drop_duplicates(keep='first')

# ## 消費交易標籤表框架2

tran_col = []
lxg_trantag2 = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)

tran_col = []
lxg_trantag2_1 = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)

tran_col = []
lxg_trantag2_2 = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)

tran_col = []
lxg_trantag2_3 = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)

# ##### 交易車款

# read 車系對照
luxgen_car = z_lxg_car_smodel

people_consumption_latest = people_consumption_latest.merge(luxgen_car, left_on='cs_carmodle', right_on='model_code',how='left')
people_consumption_3m = people_consumption_3m.merge(luxgen_car, left_on='cs_carmodle', right_on='model_code',how='left')
people_consumption_oneyear = people_consumption_oneyear.merge(luxgen_car, left_on='cs_carmodle', right_on='model_code',how='left')


# 最近1次
people_consumption_latest['cs_carmodle_f'] = np.where(people_consumption_latest['day_diff'] >= 0, people_consumption_latest['car_name'] , None )
cs_carmodle_latest = people_consumption_latest[people_consumption_latest['cs_carmodle_f'].notnull()]
cs_carmodle_latest= cs_carmodle_latest.drop_duplicates(['mobile','cs_carmodle_f'], keep='first')
# 最近3個月
people_consumption_3m['cs_carmodle_f_3m'] = np.where(people_consumption_3m['day_diff'] >= 0, people_consumption_3m['car_name'] , None)
cs_carmodle_3m = people_consumption_3m[people_consumption_3m['cs_carmodle_f_3m'].notnull()]
cs_carmodle_3m= cs_carmodle_3m.drop_duplicates(['mobile','cs_carmodle_f_3m'], keep='first')
# 最近1年
people_consumption_oneyear['cs_carmodle_f_1y'] = np.where(people_consumption_oneyear['day_diff'] >= 0, people_consumption_oneyear['car_name'], None)
cs_carmodle_1y = people_consumption_oneyear[people_consumption_oneyear['cs_carmodle_f_1y'].notnull()]
cs_carmodle_1y= cs_carmodle_1y.drop_duplicates(['mobile','cs_carmodle_f_1y'], keep='first')




lxg_trantag2 = lxg_trantag2.join(cs_carmodle_latest.loc[:,["mobile", "cs_carmodle_f"]].set_index('mobile'), on='mobile')
lxg_trantag2=lxg_trantag2[lxg_trantag2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2 = lxg_trantag2.drop_duplicates(keep='first')
lxg_trantag2 = lxg_trantag2.join(cs_carmodle_3m.loc[:,["mobile", "cs_carmodle_f_3m"]].set_index('mobile'), on='mobile')
lxg_trantag2=lxg_trantag2[lxg_trantag2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2 = lxg_trantag2.drop_duplicates(keep='first')
lxg_trantag2 = lxg_trantag2.join(cs_carmodle_1y.loc[:,["mobile", "cs_carmodle_f_1y"]].set_index('mobile'), on='mobile')
lxg_trantag2=lxg_trantag2[lxg_trantag2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2 = lxg_trantag2.drop_duplicates(keep='first')


# #### 交易乘客數


# 最近1次
people_consumption_latest['cs_carseat'] = np.where(people_consumption_latest['day_diff'] >= 0, people_consumption_latest['car_seat'] , None)
cs_carseat_latest = people_consumption_latest[people_consumption_latest['cs_carseat'].notnull()]
cs_carseat_latest= cs_carseat_latest.drop_duplicates(['mobile','cs_carseat'], keep='first')
# 最近3個月
people_consumption_3m['cs_carseat_3m'] = np.where(people_consumption_3m['day_diff'] >= 0, people_consumption_3m['car_seat'] , None)
cs_carseat_3m = people_consumption_3m[people_consumption_3m['cs_carseat_3m'].notnull()]
cs_carseat_3m= cs_carseat_3m.drop_duplicates(['mobile','cs_carseat_3m'], keep='first')
# 最近1年
people_consumption_oneyear['cs_carseat_1y'] = np.where(people_consumption_oneyear['day_diff'] >= 0, people_consumption_oneyear['car_seat'], None)
cs_carseat_1y = people_consumption_oneyear[people_consumption_oneyear['cs_carseat_1y'].notnull()]
cs_carseat_1y= cs_carseat_1y.drop_duplicates(['mobile','cs_carseat_1y'], keep='first')


lxg_trantag2 = lxg_trantag2.join(cs_carseat_latest.loc[:,["mobile", "cs_carseat"]].set_index('mobile'), on='mobile')
lxg_trantag2=lxg_trantag2[lxg_trantag2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2 = lxg_trantag2.drop_duplicates(keep='first')
lxg_trantag2 = lxg_trantag2.join(cs_carseat_3m.loc[:,["mobile", "cs_carseat_3m"]].set_index('mobile'), on='mobile')
lxg_trantag2=lxg_trantag2[lxg_trantag2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2 = lxg_trantag2.drop_duplicates(keep='first')
lxg_trantag2 = lxg_trantag2.join(cs_carseat_1y.loc[:,["mobile", "cs_carseat_1y"]].set_index('mobile'), on='mobile')
lxg_trantag2=lxg_trantag2[lxg_trantag2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2 = lxg_trantag2.drop_duplicates(keep='first')


# #### 交易車身型式


# 最近1次
people_consumption_latest['cs_carstyle'] = np.where(people_consumption_latest['day_diff'] >= 0, people_consumption_latest['car_style'] , None)
cs_carstyle_latest = people_consumption_latest[people_consumption_latest['cs_carstyle'].notnull()]
cs_carstyle_latest= cs_carstyle_latest.drop_duplicates(['mobile','cs_carstyle'], keep='first')
# 最近3個月
people_consumption_3m['cs_carstyle_3m'] = np.where(people_consumption_3m['day_diff'] >= 0, people_consumption_3m['car_style'] , None)
cs_carstyle_3m = people_consumption_3m[people_consumption_3m['cs_carstyle_3m'].notnull()]
cs_carstyle_3m= cs_carstyle_3m.drop_duplicates(['mobile','cs_carstyle_3m'], keep='first')
# 最近1年
people_consumption_oneyear['cs_carstyle_1y'] = np.where(people_consumption_oneyear['day_diff'] >= 0, people_consumption_oneyear['car_style'], None)
cs_carstyle_1y = people_consumption_oneyear[people_consumption_oneyear['cs_carstyle_1y'].notnull()]
cs_carstyle_1y= cs_carstyle_1y.drop_duplicates(['mobile','cs_carstyle_1y'], keep='first')


lxg_trantag2_3 = lxg_trantag2_3.join(cs_carstyle_latest.loc[:,["mobile", "cs_carstyle"]].set_index('mobile'), on='mobile')
lxg_trantag2_3 = lxg_trantag2_3[lxg_trantag2_3.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2_3 = lxg_trantag2_3.drop_duplicates(keep='first')
lxg_trantag2_3 = lxg_trantag2_3.join(cs_carstyle_3m.loc[:,["mobile", "cs_carstyle_3m"]].set_index('mobile'), on='mobile')
lxg_trantag2_3 = lxg_trantag2_3[lxg_trantag2_3.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2_3 = lxg_trantag2_3.drop_duplicates(keep='first')
lxg_trantag2_3 = lxg_trantag2_3.join(cs_carstyle_1y.loc[:,["mobile", "cs_carstyle_1y"]].set_index('mobile'), on='mobile')
lxg_trantag2_3 = lxg_trantag2_3[lxg_trantag2_3.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2_3 = lxg_trantag2_3.drop_duplicates(keep='first')


# #### 交易能源



# 最近1次
people_consumption_latest['cs_carenergy'] = np.where(people_consumption_latest['day_diff'] >= 0, people_consumption_latest['car_energy'] , None)
cs_carenergy_latest = people_consumption_latest[people_consumption_latest['cs_carenergy'].notnull()]
cs_carenergy_latest= cs_carenergy_latest.drop_duplicates(['mobile','cs_carenergy'], keep='first')
# 最近3個月
people_consumption_3m['cs_carenergy_3m'] = np.where(people_consumption_3m['day_diff'] >= 0, people_consumption_3m['car_energy'] , None)
cs_carenergy_3m = people_consumption_3m[people_consumption_3m['cs_carenergy_3m'].notnull()]
cs_carenergy_3m= cs_carenergy_3m.drop_duplicates(['mobile','cs_carenergy_3m'], keep='first')
# 最近1年
people_consumption_oneyear['cs_carenergy_1y'] = np.where(people_consumption_oneyear['day_diff'] >= 0, people_consumption_oneyear['car_energy'], None)
cs_carenergy_1y = people_consumption_oneyear[people_consumption_oneyear['cs_carenergy_1y'].notnull()]
cs_carenergy_1y = cs_carenergy_1y.drop_duplicates(['mobile','cs_carenergy_1y'], keep='first')




lxg_trantag2_2 = lxg_trantag2_2.join(cs_carenergy_latest.loc[:,["mobile", "cs_carenergy"]].set_index('mobile'), on='mobile')
lxg_trantag2_2 = lxg_trantag2_2[lxg_trantag2_2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2_2 = lxg_trantag2_2.drop_duplicates(keep='first')

lxg_trantag2_2 = lxg_trantag2_2.join(cs_carenergy_3m.loc[:,["mobile", "cs_carenergy_3m"]].set_index('mobile'), on='mobile')
lxg_trantag2_2 = lxg_trantag2_2[lxg_trantag2_2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2_2 = lxg_trantag2_2.drop_duplicates(keep='first')

lxg_trantag2_2 = lxg_trantag2_2.join(cs_carenergy_1y.loc[:,["mobile", "cs_carenergy_1y"]].set_index('mobile'), on='mobile')
lxg_trantag2_2 = lxg_trantag2_2[lxg_trantag2_2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2_2 = lxg_trantag2_2.drop_duplicates(keep='first')


# #### 外裝色



# 先貼外裝色，再看要不要統一色

# 最近1次
people_consumption_latest['cs_extcolor'] = np.where(people_consumption_latest['day_diff'] >= 0, people_consumption_latest['extcolour'] , None)
cs_extcolor_latest = people_consumption_latest[people_consumption_latest['cs_extcolor'].notnull()]
cs_extcolor_latest= cs_extcolor_latest.drop_duplicates(['mobile','cs_extcolor'], keep='first')
# 最近3個月
people_consumption_3m['cs_extcolor_3m'] = np.where(people_consumption_3m['day_diff'] >= 0, people_consumption_3m['extcolour'] , None)
cs_extcolor_3m = people_consumption_3m[people_consumption_3m['cs_extcolor_3m'].notnull()]
cs_extcolor_3m = cs_extcolor_3m.drop_duplicates(['mobile','cs_extcolor_3m'], keep='first')
# 最近1年
people_consumption_oneyear['cs_extcolor_1y'] = np.where(people_consumption_oneyear['day_diff'] >= 0, people_consumption_oneyear['extcolour'], None)
cs_extcolor_1y = people_consumption_oneyear[people_consumption_oneyear['cs_extcolor_1y'].notnull()]
cs_extcolor_1y = cs_extcolor_1y.drop_duplicates(['mobile','cs_extcolor_1y'], keep='first')


lxg_trantag2_1 = lxg_trantag2_1.join(cs_extcolor_latest.loc[:,["mobile", "cs_extcolor"]].set_index('mobile'), on='mobile')
lxg_trantag2_1 = lxg_trantag2_1[lxg_trantag2_1.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2_1 = lxg_trantag2_1.drop_duplicates(keep='first')
lxg_trantag2_1 = lxg_trantag2_1.join(cs_extcolor_3m.loc[:,["mobile", "cs_extcolor_3m"]].set_index('mobile'), on='mobile')
lxg_trantag2_1 = lxg_trantag2_1[lxg_trantag2_1.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2_1 = lxg_trantag2_1.drop_duplicates(keep='first')
lxg_trantag2_1 = lxg_trantag2_1.join(cs_extcolor_1y.loc[:,["mobile", "cs_extcolor_1y"]].set_index('mobile'), on='mobile')
lxg_trantag2_1 = lxg_trantag2_1[lxg_trantag2_1.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag2_1 = lxg_trantag2_1.drop_duplicates(keep='first')


# # tag_table 2



cstag_names_mapping2 = {
'yl_id' : 'yl_id',
'cs_carmodle_f': '最近一次交易_車款',
'cs_carmodle_f_3m': '近三個月交易_車款',
'cs_carmodle_f_1y': '近一年交易_車款',     
'cs_carseat': '最近一次交易_乘客數',
'cs_carseat_3m': '近三個月交易_乘客數',
'cs_carseat_1y': '近一年交易_乘客數' 
}

tmp_table2 = lxg_trantag2.loc[:, list(cstag_names_mapping2.keys())].copy()
tmp_table2.rename(columns = cstag_names_mapping2, inplace = True)




tag_table2 = tmp_table2.set_index('yl_id').stack().reset_index(name='value')
tag_table2.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table2['bcat'] = '消費紀錄'
tag_table2['mcat'] = tag_table2['tag_name'].transform(lambda x: x.split('_')[0])
tag_table2['scat'] = tag_table2['tag_name'].transform(lambda x: x.split('_')[1])
tag_table2 = tag_table2.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table2['crt_dt'] = datetime.now()
tag_table2['upd_dt'] = datetime.now()




cstag_names_mapping2_1 = {
'yl_id' : 'yl_id',
'cs_extcolor': '最近一次交易_外觀色',
'cs_extcolor_3m': '近三個月交易_外觀色',
'cs_extcolor_1y': '近一年交易_外觀色' 
# 'cs_intcolor': '最近一次交易_內裝色',
# 'cs_intcolor_3m': '近三個月交易_內裝色',
# 'cs_intcolor_1y': '近一年交易_內裝色', 
}

tmp_table2_1 = lxg_trantag2_1.loc[:, list(cstag_names_mapping2_1.keys())].copy()
tmp_table2_1.rename(columns = cstag_names_mapping2_1, inplace = True)



tag_table2_1 = tmp_table2_1.set_index('yl_id').stack().reset_index(name='value')
tag_table2_1.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table2_1['bcat'] = '消費紀錄'
tag_table2_1['mcat'] = tag_table2_1['tag_name'].transform(lambda x: x.split('_')[0])
tag_table2_1['scat'] = tag_table2_1['tag_name'].transform(lambda x: x.split('_')[1])
tag_table2_1 = tag_table2_1.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table2_1['crt_dt'] = datetime.now()
tag_table2_1['upd_dt'] = datetime.now()


cstag_names_mapping2_2 = {
'yl_id' : 'yl_id',
'cs_carenergy': '最近一次交易_能源',
'cs_carenergy_3m': '近三個月交易_能源',
'cs_carenergy_1y': '近一年交易_能源'
}

tmp_table2_2 = lxg_trantag2_2.loc[:, list(cstag_names_mapping2_2.keys())].copy()
tmp_table2_2.rename(columns = cstag_names_mapping2_2, inplace = True)


tag_table2_2 = tmp_table2_2.set_index('yl_id').stack().reset_index(name='value')
tag_table2_2.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table2_2['bcat'] = '消費紀錄'
tag_table2_2['mcat'] = tag_table2_2['tag_name'].transform(lambda x: x.split('_')[0])
tag_table2_2['scat'] = tag_table2_2['tag_name'].transform(lambda x: x.split('_')[1])
tag_table2_2 = tag_table2_1.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table2_2['crt_dt'] = datetime.now()
tag_table2_2['upd_dt'] = datetime.now()



cstag_names_mapping2_3 = {
'yl_id' : 'yl_id',
'cs_carstyle': '最近一次交易_車身型式',
'cs_carstyle_3m': '近三個月交易_車身型式',
'cs_carstyle_1y': '近一年交易_車身型式'
}

tmp_table2_3 = lxg_trantag2_3.loc[:, list(cstag_names_mapping2_3.keys())].copy()
tmp_table2_3.rename(columns = cstag_names_mapping2_3, inplace = True)



tag_table2_3 = tmp_table2_3.set_index('yl_id').stack().reset_index(name='value')
tag_table2_3.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table2_3['bcat'] = '消費紀錄'
tag_table2_3['mcat'] = tag_table2_3['tag_name'].transform(lambda x: x.split('_')[0])
tag_table2_3['scat'] = tag_table2_3['tag_name'].transform(lambda x: x.split('_')[1])
tag_table2_3 = tag_table2_3.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table2_3['crt_dt'] = datetime.now()
tag_table2_3['upd_dt'] = datetime.now()




tag_table2 = pd.concat([tag_table2, tag_table2_1,tag_table2_2, tag_table2_3], ignore_index=True)




tag_table2 = tag_table2.drop_duplicates(keep='first')


# # 消費交易標籤表框架3


tran_col = []
lxg_trantag3 = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)

tran_col = []
lxg_trantag3_1 = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)




tran_col = []
lxg_trantag3_2 = tran_id.reindex(columns = tran_id.columns.tolist() + tran_col)


# #### 內裝色


# 先貼外裝色，再看要不要統一色

# 最近1次
people_consumption_latest['cs_intcolor'] = np.where(people_consumption_latest['day_diff'] >= 0, people_consumption_latest['intcolour'] , None)
cs_intcolor_latest = people_consumption_latest[people_consumption_latest['cs_intcolor'].notnull()]
cs_intcolor_latest= cs_intcolor_latest.drop_duplicates(['mobile','cs_intcolor'], keep='first')
# 最近3個月
people_consumption_3m['cs_intcolor_3m'] = np.where(people_consumption_3m['day_diff'] >= 0, people_consumption_3m['intcolour'] , None)
cs_intcolor_3m = people_consumption_3m[people_consumption_3m['cs_intcolor_3m'].notnull()]
cs_intcolor_3m= cs_intcolor_3m.drop_duplicates(['mobile','cs_intcolor_3m'], keep='first')
# 最近1年
people_consumption_oneyear['cs_intcolor_1y'] = np.where(people_consumption_oneyear['day_diff'] >= 0, people_consumption_oneyear['intcolour'], None)
cs_intcolor_1y = people_consumption_oneyear[people_consumption_oneyear['cs_intcolor_1y'].notnull()]
cs_intcolor_1y= cs_intcolor_1y.drop_duplicates(['mobile','cs_intcolor_1y'], keep='first')



lxg_trantag3 = lxg_trantag3.join(cs_intcolor_latest.loc[:,["mobile", "cs_intcolor"]].set_index('mobile'), on='mobile')
lxg_trantag3 = lxg_trantag3[lxg_trantag3.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag3 = lxg_trantag3.drop_duplicates(keep='first')

lxg_trantag3 = lxg_trantag3.join(cs_intcolor_3m.loc[:,["mobile", "cs_intcolor_3m"]].set_index('mobile'), on='mobile')
lxg_trantag3 = lxg_trantag3[lxg_trantag3.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag3 = lxg_trantag3.drop_duplicates(keep='first')

lxg_trantag3 = lxg_trantag3.join(cs_intcolor_1y.loc[:,["mobile", "cs_intcolor_1y"]].set_index('mobile'), on='mobile')
lxg_trantag3 = lxg_trantag3[lxg_trantag3.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag3 = lxg_trantag3.drop_duplicates(keep='first')


# #### 最近消費金額分級(百分位)



# 最近1次
# people_consumption_latest['cs_cost_pr'] = pd.to_numeric(people_consumption_latest['cs_cost'], errors='coerce').rank(pct=True)
people_consumption_latest['cs_cost_pr'] = pd.qcut(people_consumption_latest['cs_cost'], 100, labels = False, duplicates='drop') 
cs_cost_pr_latest = people_consumption_latest[people_consumption_latest['cs_cost_pr'].notnull()]
cs_cost_pr_latest= cs_cost_pr_latest.drop_duplicates(['mobile','cs_cost_pr'], keep='first')

# 近3個月
# consumption_pricefre_3m['cs_cost_pr_3m'] = pd.to_numeric(consumption_pricefre_3m['cost_3m'], errors='coerce').rank(pct=True)
consumption_pricefre_3m['cs_cost_pr_3m'] = pd.qcut(consumption_pricefre_3m['cost_3m'], 100, labels = False, duplicates='drop')
cs_cost_pr_3m = consumption_pricefre_3m[consumption_pricefre_3m['cs_cost_pr_3m'].notnull()]
cs_cost_pr_3m= cs_cost_pr_3m.drop_duplicates(['mobile','cs_cost_pr_3m'], keep='first')

# 近1年
# consumption_pricefre_oneyear['cs_cost_pr_1y'] = pd.to_numeric(consumption_pricefre_oneyear['cost_1y'], errors='coerce').rank(pct=True)
consumption_pricefre_oneyear['cs_cost_pr_1y'] = pd.qcut(consumption_pricefre_oneyear['cost_1y'], 100, labels = False, duplicates='drop')
cs_cost_pr_1y = consumption_pricefre_oneyear[consumption_pricefre_oneyear['cs_cost_pr_1y'].notnull()]
cs_cost_pr_1y= cs_cost_pr_1y.drop_duplicates(['mobile','cs_cost_pr_1y'], keep='first')



lxg_trantag3_1 = lxg_trantag3_1.join(cs_cost_pr_latest.loc[:,["mobile", "cs_cost_pr"]].set_index('mobile'), on='mobile')

lxg_trantag3_1 = lxg_trantag3_1[lxg_trantag3_1.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag3_1 = lxg_trantag3_1.drop_duplicates(keep='first')

lxg_trantag3_1 = lxg_trantag3_1.join(cs_cost_pr_3m.loc[:,["mobile", "cs_cost_pr_3m"]].set_index('mobile'), on='mobile')
lxg_trantag3_1 = lxg_trantag3_1[lxg_trantag3_1.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag3_1 = lxg_trantag3_1.drop_duplicates(keep='first')

# cs_cost_pr_1y = cs_cost_pr_1y[cs_cost_pr_1y.iloc[:, 2:].notnull().any(axis=1)]
# cs_cost_pr_1y = cs_cost_pr_1y.drop_duplicates(keep='first')

# lxg_trantag3_1 = lxg_trantag3_1.join(cs_cost_pr_1y.loc[:,["mobile", "cs_cost_pr_1y"]].set_index('mobile'), on='mobile')
# lxg_trantag3_1 = lxg_trantag3_1[lxg_trantag3_1.iloc[:, 2:].notnull().any(axis=1)]
# lxg_trantag3_1 = lxg_trantag3_1.drop_duplicates(keep='first')




cs_cost_pr_1y = cs_cost_pr_1y[cs_cost_pr_1y.iloc[:, 2:].notnull().any(axis=1)]
cs_cost_pr_1y = cs_cost_pr_1y.drop_duplicates(keep='first')

lxg_trantag3_2 = lxg_trantag3_2.join(cs_cost_pr_1y.loc[:,["mobile", "cs_cost_pr_1y"]].set_index('mobile'), on='mobile')
lxg_trantag3_2 = lxg_trantag3_2[lxg_trantag3_2.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag3_2 = lxg_trantag3_2.drop_duplicates(keep='first')


# #### 最近消費金額區間



cost_ref_bins = [0, 2000, 4000, 6000, 8000, 10000 , np.inf]
ref_interval_name = ['0-2000', '2000-4000', '4000-6000', '6000-8000', '8000-10000', '>10000']

# 最近1次
people_consumption_latest['cs_cost_interval'] = pd.cut(people_consumption_latest['cs_cost'], cost_ref_bins, labels=ref_interval_name)
cs_cost_interval_latest = people_consumption_latest[people_consumption_latest['cs_cost_interval'].notnull()]
cs_cost_interval_latest= cs_cost_interval_latest.drop_duplicates(['mobile','cs_cost_interval'], keep='first')
# 近3個月
consumption_pricefre_3m['cs_cost_interval_3m'] = pd.cut(consumption_pricefre_3m['cost_3m'], cost_ref_bins, labels=ref_interval_name)
cs_cost_interval_3m = consumption_pricefre_3m[consumption_pricefre_3m['cs_cost_interval_3m'].notnull()]
cs_cost_interval_3m= cs_cost_interval_3m.drop_duplicates(['mobile','cs_cost_interval_3m'], keep='first')
# 近1年
consumption_pricefre_oneyear['cs_cost_interval_1y'] = pd.cut(consumption_pricefre_oneyear['cost_1y'], cost_ref_bins, labels=ref_interval_name)
cs_cost_interval_1y = consumption_pricefre_oneyear[consumption_pricefre_oneyear['cs_cost_interval_1y'].notnull()]
cs_cost_interval_1y= cs_cost_interval_1y.drop_duplicates(['mobile','cs_cost_interval_1y'], keep='first')


lxg_trantag3 = lxg_trantag3.join(cs_cost_interval_latest.loc[:,["mobile", "cs_cost_interval"]].set_index('mobile'), on='mobile')
lxg_trantag3 = lxg_trantag3[lxg_trantag3.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag3 = lxg_trantag3.drop_duplicates(keep='first')
lxg_trantag3 = lxg_trantag3.join(cs_cost_interval_3m.loc[:,["mobile", "cs_cost_interval_3m"]].set_index('mobile'), on='mobile')
lxg_trantag3 = lxg_trantag3[lxg_trantag3.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag3 = lxg_trantag3.drop_duplicates(keep='first')
lxg_trantag3 = lxg_trantag3.join(cs_cost_interval_1y.loc[:,["mobile", "cs_cost_interval_1y"]].set_index('mobile'), on='mobile')
lxg_trantag3 = lxg_trantag3[lxg_trantag3.iloc[:, 2:].notnull().any(axis=1)]
lxg_trantag3 = lxg_trantag3.drop_duplicates(keep='first')


# # tag_table 3



cstag_names_mapping3 = {
'yl_id' : 'yl_id',
'cs_intcolor': '最近一次交易_內裝色',
'cs_intcolor_3m': '近三個月交易_內裝色',
'cs_intcolor_1y': '近一年交易_內裝色',
'cs_cost_interval': '近一年交易_交易金額區間',
'cs_cost_interval_3m': '近三個月交易_交易金額區間',
'cs_cost_interval_1y': '近一年交易_交易金額區間'
}

tmp_table3 = lxg_trantag3.loc[:, list(cstag_names_mapping3.keys())].copy()
tmp_table3.rename(columns = cstag_names_mapping3, inplace = True)


# # Reshape



tag_table3 = tmp_table3.set_index('yl_id').stack().reset_index(name='value')
tag_table3.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table3['bcat'] = '消費紀錄'
tag_table3['mcat'] = tag_table3['tag_name'].transform(lambda x: x.split('_')[0])
tag_table3['scat'] = tag_table3['tag_name'].transform(lambda x: x.split('_')[1])
tag_table3 = tag_table3.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table3['crt_dt'] = datetime.now()
tag_table3['upd_dt'] = datetime.now()
tag_table3['crt_src'] = datetime.now()




cstag_names_mapping3_1 = {
'yl_id' : 'yl_id',
'cs_cost_pr': '近一年交易_消費金額分級',
'cs_cost_pr_3m': '近三個月交易_消費金額分級'
}

tmp_table3_1 = lxg_trantag3_1.loc[:, list(cstag_names_mapping3_1.keys())].copy()
tmp_table3_1.rename(columns = cstag_names_mapping3_1, inplace = True)




tag_table3_1 = tmp_table3_1.set_index('yl_id').stack().reset_index(name='value')
tag_table3_1.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table3_1['bcat'] = '消費紀錄'
tag_table3_1['mcat'] = tag_table3_1['tag_name'].transform(lambda x: x.split('_')[0])
tag_table3_1['scat'] = tag_table3_1['tag_name'].transform(lambda x: x.split('_')[1])
tag_table3_1 = tag_table3_1.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table3_1['crt_dt'] = datetime.now()
tag_table3_1['upd_dt'] = datetime.now()
tag_table3_1['crt_src'] = datetime.now()



cstag_names_mapping3_2 = {
'yl_id' : 'yl_id',
'cs_cost_pr_1y': '近一年交易_消費金額分級'
}

tmp_table3_2 = lxg_trantag3_2.loc[:, list(cstag_names_mapping3_2.keys())].copy()
tmp_table3_2.rename(columns = cstag_names_mapping3_2, inplace = True)



tag_table3_2 = tmp_table3_2.set_index('yl_id').stack().reset_index(name='value')
tag_table3_2.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table3_2['bcat'] = '消費紀錄'
tag_table3_2['mcat'] = tag_table3_2['tag_name'].transform(lambda x: x.split('_')[0])
tag_table3_2['scat'] = tag_table3_2['tag_name'].transform(lambda x: x.split('_')[1])
tag_table3_2 = tag_table3_2.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table3_2['crt_dt'] = datetime.now()
tag_table3_2['upd_dt'] = datetime.now()
tag_table3_2['crt_src'] = datetime.now()




tag_table3 = pd.concat([tag_table3, tag_table3_1, tag_table3_2], ignore_index=True)




tag_table3 = tag_table3.drop_duplicates(keep='first')


# # concat tag_table 1-3


tag_table = pd.concat([tag_table1, tag_table2, tag_table3], ignore_index=True)
tag_table = tag_table[tag_table["tag_value"] != '未知']
tag_table = tag_table[tag_table["tag_value"].notnull()]
tag_table = tag_table[tag_table["tag_value"] != '']

tag_table = tag_table.drop_duplicates(keep='first')

brand_tag_define_lxg = brand_tag_define[(brand_tag_define['brand_name'] == '納智捷')]
brand_tag_define_lxg = brand_tag_define_lxg[ brand_tag_define_lxg['bcat'] == '消費紀錄']
brand_tag_define_lxg = brand_tag_define_lxg[ brand_tag_define_lxg['stp_dt'].isna()]

tag_table = tag_table.join(brand_tag_define_lxg.loc[:,["tag_name", "tag_id"]].set_index('tag_name'), on='tag_name')
tag_table = tag_table.drop_duplicates(keep='first')



cmain_lxg_people_lxgid = cmain_lxg_people_lxgid[["yl_id", "memid"]]
tag_table = tag_table.join(cmain_lxg_people_lxgid.loc[:,["memid", "yl_id"]].set_index('yl_id'), on='yl_id')



tag_table = tag_table.drop_duplicates(keep='first')
tag_table.rename(columns={'memid':'lxg_id'}, inplace=True )


tag_table_test02 = tag_table.head(10000)
len(tag_table_test02)
print(tag_table_test02)

for i in range(0,len(tag_table_test02),10000):
    write_into_tag_table(df = tag_table_test02.iloc[i:i+10000], table_name = 'lxg_tag', schema_name = 'dt_tag', method = 'keep')
    print('The batches are:', '[',  i,  + i+10000, ']', '\n')