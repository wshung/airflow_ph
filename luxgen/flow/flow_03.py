
import os
import numpy as np
import pandas as pd
import psycopg2
import warnings
warnings.filterwarnings('ignore')
from datetime import date
from datetime import datetime
today = date.today()

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


# 納智捷人表
query_string = """
SELECT yl_id,
sso_id,
pii_token
FROM info.cmain_lxg_people
;
"""
cmain_lxg_people = doing_query(query_string)

# phone_to_id
query_string = """
select 
pii_token,phone, convert_from(decrypt(decode(phone, 'base64'),'evm$9xyyBar%8ZpRSMNdQEgwZrdY8qFx','aes-ecb'), 'UTF8') AS de_columnname
from
info.phone_to_id;
;
"""
cmain_lxg_pii_de = doing_query(query_string)

# 有望客表
query_string = """
SELECT cst_mphone_09,
dlr_code,
dept_code,
ist_model,
crt_date,
lead_sourcedesc
FROM xdt_lxg.lxg_nv_opp_cst
where crt_date >= now() - interval '365 day'
;
"""
lxg_nv_opp_cst = doing_query(query_string)


# testdrive
query_string = """
SELECT mobileno_09,
dealercode,
deptcode,
modelcode,
testdrivedate
FROM xdt_lxg.lxg_nv_testdrive
where testdrivedate >= now() - interval '365 day'
;
"""
lxg_nv_testdrive = doing_query(query_string)

# crm
query_string = """
SELECT opendate,
mobilephone_09,
crm_type,
crm_model
FROM xdt_lxg.lxg_nv_crm
where opendate >= now() - interval '365 day'
;
"""
lxg_nv_crm = doing_query(query_string)

# # web_member
# query_string = """
# SELECT line_uid,
# phone,
# pk
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

z_lxg_car_smodel = pd.read_excel('./納智捷車系對照.xlsx' )

# z_ylg_postcode

z_ylg_postcode = pd.read_excel('./z_ylg_postcode.xlsx' )

# brand_tag_define
query_string = """
SELECT *
FROM dt_tag.brand_tag_define
;
"""
brand_tag_define = doing_query(query_string)




from utils.helpers import get_YN, get_YNO, delete_old_tag, write_into_tag_table





cmain_lxg_pii_de.rename(columns={'phone': 'mobile'}, inplace=True)
cmain_lxg_people = cmain_lxg_people.merge(cmain_lxg_pii_de, left_on='pii_token', right_on='pii_token',how='left')





lxg_nweb_user_members.rename(columns={'luxgenuserid': 'uuid', 'lineuserid': 'line_uid', }, inplace=True)
lxg_nweb_user_members = lxg_nweb_user_members.merge(lxg_nweb_user_member_infos, left_on='memid', right_on='memid',how='left')
cmain_lxg_pii_de_phone = cmain_lxg_pii_de[['mobile','de_columnname']]
lxg_nweb_user_members = lxg_nweb_user_members.merge(cmain_lxg_pii_de_phone, left_on='phone_09', right_on='mobile',how='left')





cmain_lxg_people_lxgid = cmain_lxg_people[['yl_id', 'sso_id' ,'mobile']]
lxg_nweb_user_members_memid = lxg_nweb_user_members[['memid' ,'mobile']]
cmain_lxg_people_lxgid = cmain_lxg_people_lxgid.merge(lxg_nweb_user_members_memid, left_on='mobile', right_on='mobile',how='left')


# ## 顧關資料整理

# #### 有望客資料

# ##### 最近1年




# 計算成為有望客日期和資料更新日期的差幾天(days)
lxg_nv_opp_cst['day_diff'] =  (today - pd.to_datetime(lxg_nv_opp_cst['crt_date']).dt.date)
lxg_nv_opp_cst['day_diff'] =  lxg_nv_opp_cst['day_diff'].dt.days

# 篩選1年內的有望客
lxg_nv_opp_cst_oneyear =lxg_nv_opp_cst[(lxg_nv_opp_cst['day_diff'] >= 0 ) & (lxg_nv_opp_cst['day_diff'] <=365)]

lxg_nv_opp_cst_oneyear['mobile'] = lxg_nv_opp_cst_oneyear['cst_mphone_09']
lxg_nv_opp_cst_oneyear['offline_brand'] = '納智捷'
lxg_nv_opp_cst_oneyear['offline_type'] = '新車'
lxg_nv_opp_cst_oneyear['offline_item'] = '有望客'
lxg_nv_opp_cst_oneyear['offline_dlrcode'] = lxg_nv_opp_cst_oneyear['dlr_code']
lxg_nv_opp_cst_oneyear['offline_deptcode'] = lxg_nv_opp_cst_oneyear['dept_code']
lxg_nv_opp_cst_oneyear['offline_carmodle'] = lxg_nv_opp_cst_oneyear['ist_model']
lxg_nv_opp_cst_oneyear['offline_visitdate'] = lxg_nv_opp_cst_oneyear['crt_date']
lxg_nv_opp_cst_oneyear['offline_visitdate_tf'] = lxg_nv_opp_cst_oneyear['crt_date'].dt.date
lxg_nv_opp_cst_oneyear['offline_dayname'] = lxg_nv_opp_cst_oneyear['crt_date'].dt.day_name()
lxg_nv_opp_cst_oneyear['offline_channel'] = lxg_nv_opp_cst_oneyear['lead_sourcedesc']


opp_cst_oneyear= lxg_nv_opp_cst_oneyear[[ 'mobile', 'day_diff','offline_brand', 'offline_type', 'offline_item', 'offline_dlrcode', 
'offline_deptcode', 'offline_carmodle', 'offline_visitdate', 'offline_visitdate_tf', 'offline_dayname', 'offline_channel']]


# #### 試駕資料

# ##### 最近1年




# 計算成為試駕日期和資料更新日期的差幾天(days)
lxg_nv_testdrive['day_diff'] =  today - pd.to_datetime(lxg_nv_testdrive['testdrivedate']).dt.date
lxg_nv_testdrive['day_diff'] =  lxg_nv_testdrive['day_diff'].dt.days
# 篩選1年內的訂單
lxg_nv_testdrive_oneyear = lxg_nv_testdrive[(lxg_nv_testdrive['day_diff'] >= 0 ) & (lxg_nv_testdrive['day_diff'] <=365)]

lxg_nv_testdrive_oneyear['mobile'] = lxg_nv_testdrive_oneyear['mobileno_09']
lxg_nv_testdrive_oneyear['offline_brand'] = '納智捷'
lxg_nv_testdrive_oneyear['offline_type'] = '新車'
lxg_nv_testdrive_oneyear['offline_item'] = '試駕'
lxg_nv_testdrive_oneyear['offline_dlrcode'] = lxg_nv_testdrive_oneyear['dealercode']
lxg_nv_testdrive_oneyear['offline_deptcode'] = lxg_nv_testdrive_oneyear['deptcode']
lxg_nv_testdrive_oneyear['offline_carmodle'] = lxg_nv_testdrive_oneyear['modelcode']
lxg_nv_testdrive_oneyear['offline_visitdate'] =pd.to_datetime(lxg_nv_testdrive_oneyear['testdrivedate'])
lxg_nv_testdrive_oneyear['offline_visitdate_tf'] =pd.to_datetime(lxg_nv_testdrive_oneyear['testdrivedate'])
lxg_nv_testdrive_oneyear['offline_dayname'] = pd.to_datetime(lxg_nv_testdrive_oneyear['testdrivedate']).dt.day_name()
lxg_nv_testdrive_oneyear['offline_channel'] = '未知'


testdrive_oneyear= lxg_nv_testdrive_oneyear[[ 'mobile', 'day_diff', 'offline_brand', 'offline_type', 'offline_item', 'offline_dlrcode', 
'offline_deptcode', 'offline_carmodle', 'offline_visitdate', 'offline_visitdate_tf', 'offline_dayname', 'offline_channel']]


# #### CRM資料

# ##### 最近1年



# 計算成為試駕日期和資料更新日期的差幾天(days)
lxg_nv_crm['day_diff'] =  (today - pd.to_datetime(lxg_nv_crm['opendate']).dt.date)
lxg_nv_crm['day_diff'] =  lxg_nv_crm['day_diff'].dt.days
# 篩選1年內的訂單
lxg_nv_crm_oneyear = lxg_nv_crm[(lxg_nv_crm['day_diff'] >= 0 ) & (lxg_nv_crm['day_diff'] <=365)]

lxg_nv_crm_oneyear['mobile'] = lxg_nv_crm_oneyear['mobilephone_09']
lxg_nv_crm_oneyear['offline_brand'] = '納智捷'
lxg_nv_crm_oneyear['offline_type'] = '客服'
lxg_nv_crm_oneyear['offline_item'] = lxg_nv_crm_oneyear['crm_type']
lxg_nv_crm_oneyear['offline_dlrcode'] = '未知'
lxg_nv_crm_oneyear['offline_deptcode'] = '未知'
lxg_nv_crm_oneyear['offline_carmodle'] = lxg_nv_crm_oneyear['crm_model']
lxg_nv_crm_oneyear['offline_visitdate'] =lxg_nv_crm_oneyear['opendate']
lxg_nv_crm_oneyear['offline_visitdate_tf'] =lxg_nv_crm_oneyear['opendate'].dt.date
lxg_nv_crm_oneyear['offline_dayname'] = lxg_nv_crm_oneyear['opendate'].dt.day_name()
lxg_nv_crm_oneyear['offline_channel'] = '未知'


crm_oneyear= lxg_nv_crm_oneyear[[ 'mobile','day_diff' , 'offline_brand', 'offline_type', 'offline_item', 'offline_dlrcode', 
'offline_deptcode', 'offline_carmodle', 'offline_visitdate', 'offline_visitdate_tf', 'offline_dayname', 'offline_channel']]


# #### Join 有望客, 試駕和CRM




offline_df_oneyear = pd.concat([opp_cst_oneyear, testdrive_oneyear, crm_oneyear], ignore_index=True)





people_offline_df_oneyear = cmain_lxg_people.merge(offline_df_oneyear, left_on='mobile', right_on='mobile',how='left')


# #### 近3個月資料




people_offline_df_3m = people_offline_df_oneyear[(people_offline_df_oneyear['day_diff'] >= 0) & (people_offline_df_oneyear['day_diff'] <= 90)]


# #### 最近1次




offline_df_latest_tm = offline_df_oneyear
offline_df_latest_tm['offline_visitdate_tf'] = pd.to_datetime(offline_df_latest_tm.offline_visitdate_tf)
offline_df_latest = offline_df_latest_tm.loc[offline_df_latest_tm.groupby('mobile')['offline_visitdate_tf'].idxmax()]
people_offline_df_latest = cmain_lxg_people.merge(offline_df_latest, left_on='mobile', right_on='mobile',how='left')


# ### 顧關管理標籤框架



cmain_lxg_people_id = cmain_lxg_people.copy()
mem_col = []
offline_tag = cmain_lxg_people_id.reindex(columns = cmain_lxg_people_id.columns.tolist() + mem_col)





cmain_lxg_people_id = cmain_lxg_people.copy()
mem_col = []
offline_tag1 = cmain_lxg_people_id.reindex(columns = cmain_lxg_people_id.columns.tolist() + mem_col)


# #### 接觸品牌




# 最近1次
people_offline_df_latest['offline_brand_f'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['offline_brand'], None)
people_offline_df_latest = people_offline_df_latest[people_offline_df_latest['offline_brand_f'].notnull()]
# 最近3個月
people_offline_df_3m['offline_brand_3m'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['offline_brand'], None)
people_offline_df_3m = people_offline_df_3m[people_offline_df_3m['offline_brand_3m'].notnull()]
# 最近1年
people_offline_df_oneyear['offline_brand_1y'] = np.where(people_offline_df_oneyear['day_diff'] >= 0, people_offline_df_oneyear['offline_brand'], None)
people_offline_df_oneyear = people_offline_df_oneyear[people_offline_df_oneyear['offline_brand_1y'].notnull()]





offline_tag = offline_tag.join(people_offline_df_latest.notnull().loc[:,["mobile", "offline_brand_f"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_3m.loc[:,["mobile", "offline_brand_3m"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_oneyear.loc[:,["mobile", "offline_brand_1y"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')


# #### 接觸大項



# 最近1次
people_offline_df_latest['offline_type_f'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['offline_type'], None)
people_offline_df_latest = people_offline_df_latest[people_offline_df_latest['offline_type_f'].notnull()]
# 最近3個月
people_offline_df_3m['offline_type_3m'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['offline_type'], None )
people_offline_df_3m = people_offline_df_3m[people_offline_df_3m['offline_type_3m'].notnull()]
# 最近1年
people_offline_df_oneyear['offline_type_1y'] = np.where(people_offline_df_oneyear['day_diff'] >= 0, people_offline_df_oneyear['offline_type'], None)
people_offline_df_oneyear = people_offline_df_oneyear[people_offline_df_oneyear['offline_type_1y'].notnull()]





offline_tag = offline_tag.join(people_offline_df_latest.loc[:,["mobile", "offline_type_f"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_3m.loc[:,["mobile", "offline_type_3m"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_oneyear.loc[:,["mobile", "offline_type_1y"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')


# #### 接觸項目




# 最近1次
people_offline_df_latest['offline_item_f'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['offline_item'], None)
people_offline_df_latest = people_offline_df_latest[people_offline_df_latest['offline_item_f'].notnull()]
# 最近3個月
people_offline_df_3m['offline_item_3m'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['offline_item'], None)
people_offline_df_3m = people_offline_df_3m[people_offline_df_3m['offline_item_3m'].notnull()]
# 最近1年
people_offline_df_oneyear['offline_item_1y'] = np.where(people_offline_df_oneyear['day_diff'] >= 0, people_offline_df_oneyear['offline_item'], None)
people_offline_df_oneyear = people_offline_df_oneyear[people_offline_df_oneyear['offline_item_1y'].notnull()]




offline_tag = offline_tag.join(people_offline_df_latest.loc[:,["mobile", "offline_item_f"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_3m.loc[:,["mobile", "offline_item_3m"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_oneyear.loc[:,["mobile", "offline_item_1y"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')


# #### 車款



# join 車系
luxgen_car = z_lxg_car_smodel
people_offline_df_latest = people_offline_df_latest.merge(luxgen_car, left_on='offline_carmodle', right_on='model_code',how='left')
people_offline_df_3m = people_offline_df_3m.merge(luxgen_car, left_on='offline_carmodle', right_on='model_code',how='left')
people_offline_df_oneyear = people_offline_df_oneyear.merge(luxgen_car, left_on='offline_carmodle', right_on='model_code',how='left')




# 最近1次
people_offline_df_latest['offline_carmodle_f'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['car_name'] , None)
people_offline_df_latest = people_offline_df_latest[people_offline_df_latest['offline_carmodle_f'].notnull()]
# 最近3個月
people_offline_df_3m['offline_carmodle_3m'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['car_name'] , None)
people_offline_df_3m = people_offline_df_3m[people_offline_df_3m['offline_carmodle_3m'].notnull()]
# 最近1年
people_offline_df_oneyear['offline_carmodle_1y'] = np.where(people_offline_df_oneyear['day_diff'] >= 0, people_offline_df_oneyear['car_name'], None)
people_offline_df_oneyear = people_offline_df_oneyear[people_offline_df_oneyear['offline_carmodle_1y'].notnull()]





offline_tag = offline_tag.join(people_offline_df_latest.loc[:,["mobile", "offline_carmodle_f"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_3m.loc[:,["mobile", "offline_carmodle_3m"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_oneyear.loc[:,["mobile", "offline_carmodle_1y"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')


# #### 能源



# 最近1次
people_offline_df_latest['cs_carenergy'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['car_energy'] , None)
people_offline_df_latest = people_offline_df_latest[people_offline_df_latest['cs_carenergy'].notnull()]
# 最近3個月
people_offline_df_3m['cs_carenergy_3m'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['car_energy'] , None)
people_offline_df_3m = people_offline_df_3m[people_offline_df_3m['cs_carenergy_3m'].notnull()]
# 最近1年
people_offline_df_oneyear['cs_carenergy_1y'] = np.where(people_offline_df_oneyear['day_diff'] >= 0, people_offline_df_oneyear['car_energy'], None)
people_offline_df_oneyear = people_offline_df_oneyear[people_offline_df_oneyear['cs_carenergy_1y'].notnull()]




offline_tag = offline_tag.join(people_offline_df_latest.loc[:,["mobile", "cs_carenergy"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_3m.loc[:,["mobile", "cs_carenergy_3m"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_oneyear.loc[:,["mobile", "cs_carenergy_1y"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')


# #### 車身型式



# 最近1次
people_offline_df_latest['cs_carstyle'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['car_style'] , None)
people_offline_df_latest = people_offline_df_latest[people_offline_df_latest['cs_carstyle'].notnull()]
# 最近3個月
people_offline_df_3m['cs_carstyle_3m'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['car_style'] , None)
people_offline_df_3m = people_offline_df_3m[people_offline_df_3m['cs_carstyle_3m'].notnull()]
# 最近1年
people_offline_df_oneyear['cs_carstyle_1y'] = np.where(people_offline_df_oneyear['day_diff'] >= 0, people_offline_df_oneyear['car_style'], None)
people_offline_df_oneyear = people_offline_df_oneyear[people_offline_df_oneyear['cs_carstyle_1y'].notnull()]




offline_tag = offline_tag.join(people_offline_df_latest.loc[:,["mobile", "cs_carstyle"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_3m.loc[:,["mobile", "cs_carstyle_3m"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_oneyear.loc[:,["mobile", "cs_carstyle_1y"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')


# #### 乘客數



# 最近1次
people_offline_df_latest['cs_carseat'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['car_seat'] , None)
people_offline_df_latest = people_offline_df_latest[people_offline_df_latest['cs_carseat'].notnull()]
# 最近3個月
people_offline_df_3m['cs_carseat_3m'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['car_seat'] , None)
people_offline_df_3m = people_offline_df_3m[people_offline_df_3m['cs_carseat_3m'].notnull()]
# 最近1年
people_offline_df_oneyear['cs_carseat_1y'] = np.where(people_offline_df_oneyear['day_diff'] >= 0, people_offline_df_oneyear['car_seat'], None)
people_offline_df_oneyear = people_offline_df_oneyear[people_offline_df_oneyear['cs_carseat_1y'].notnull()]



offline_tag = offline_tag.join(people_offline_df_latest.loc[:,["mobile", "cs_carseat"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_3m.loc[:,["mobile", "cs_carseat_3m"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')

offline_tag = offline_tag.join(people_offline_df_oneyear.loc[:,["mobile", "cs_carseat_1y"]].set_index('mobile'), on='mobile')
offline_tag=offline_tag[offline_tag.iloc[:, 2:].notnull().any(axis=1)]
offline_tag = offline_tag.drop_duplicates(keep='first')


# #### 接觸天數

# #### 客服諮詢類型


crm_oneyear_tm = crm_oneyear
crm_oneyear_tm['offline_visitdate_tf'] = pd.to_datetime(crm_oneyear_tm.offline_visitdate_tf)
crm_df_latest = crm_oneyear_tm.loc[crm_oneyear_tm.groupby('mobile')['offline_visitdate_tf'].idxmax()]
people_crm_df_latest = cmain_lxg_people.merge(crm_df_latest, left_on='mobile', right_on='mobile',how='left')

# 最近一年
people_crm_df_oneyear= cmain_lxg_people.merge(crm_oneyear_tm, left_on='mobile', right_on='mobile',how='left')
# 最近3個月
people_crm_df_3m= people_crm_df_oneyear[(people_crm_df_oneyear['day_diff'] >= 0) & (people_crm_df_oneyear['day_diff'] <= 90)]




# 最近1次
people_crm_df_latest['offline_crm_type'] = np.where(people_crm_df_latest['day_diff'] >= 0, people_crm_df_latest['offline_type'] , '' )
people_crm_df_latest = people_crm_df_latest[people_crm_df_latest['offline_crm_type'].notnull()]

# 最近3個月(前3)
people_crm_df_3m['crm_type_yn'] = np.where(people_crm_df_3m['day_diff'] >= 0, 1 , 0)
people_crm_df_3m_typesum = people_crm_df_3m.groupby(['mobile','offline_item'])['crm_type_yn'].sum().reset_index(name="crm_type_sum_3m")
people_crm_df_3m_typesum_f3 = people_crm_df_3m_typesum.sort_values('crm_type_sum_3m', ascending = False).groupby('mobile').head(3)
people_crm_df_3m_typesum_f3 = people_crm_df_3m_typesum_f3[people_crm_df_3m_typesum_f3['crm_type_sum_3m'] != 0]
people_crm_df_3m_typesum_f3 = people_crm_df_3m_typesum_f3.rename(columns={'offline_item' : 'offline_crm_item_3m'})
# 最近1年(前3)
people_crm_df_oneyear['crm_type_yn'] = np.where(people_crm_df_oneyear['day_diff'] >= 0, 1 , 0)
people_crm_df_oneyear_typesum = people_crm_df_oneyear.groupby(['mobile','offline_item'])['crm_type_yn'].sum().reset_index(name="crm_type_sum_1y")
people_crm_df_oneyear_typesum_f3 = people_crm_df_oneyear_typesum.sort_values('crm_type_sum_1y', ascending = False).groupby('mobile').head(3)
people_crm_df_oneyear_typesum_f3 = people_crm_df_oneyear_typesum_f3[people_crm_df_oneyear_typesum_f3['crm_type_sum_1y'] != 0]
people_crm_df_oneyear_typesum_f3 = people_crm_df_oneyear_typesum_f3.rename(columns={'offline_item' : 'offline_crm_item_1y'})




offline_tag1 = offline_tag1.join(people_crm_df_latest.loc[:,["mobile", "offline_crm_type"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')

offline_tag1 = offline_tag1.join(people_crm_df_3m_typesum_f3.loc[:,["mobile",  "offline_crm_item_3m"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')

offline_tag1 = offline_tag1.join(people_crm_df_oneyear_typesum_f3.loc[:,["mobile",  "offline_crm_item_1y"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')


# #### 客服次數



# 最近3個月CRM次數 - 確認
people_crm_df_3m['crm_freq_yn'] = np.where(people_crm_df_3m['day_diff'] >= 0, 1 , 0)
people_crm_df_3m_tfreq = people_crm_df_3m.groupby('mobile')['crm_freq_yn'].sum().reset_index(name="crm_freq_3m")
people_crm_df_3m_tfreq = people_crm_df_3m_tfreq[people_crm_df_3m_tfreq['crm_freq_3m'] != 0]

# 最近1年CRM次數
people_crm_df_oneyear['crm_freq_yn'] = np.where(people_crm_df_oneyear['day_diff'] >= 0, 1 , 0)
people_crm_df_oneyear_tfreq = people_crm_df_oneyear.groupby('mobile')['crm_freq_yn'].sum().reset_index(name="crm_freq_1y")
people_crm_df_oneyear_tfreq = people_crm_df_oneyear_tfreq[people_crm_df_oneyear_tfreq['crm_freq_1y'] != 0]




offline_tag1 = offline_tag1.join(people_crm_df_3m_tfreq.loc[:,["mobile",  "crm_freq_3m"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')

offline_tag1 = offline_tag1.join(people_crm_df_oneyear_tfreq.loc[:,["mobile",  "crm_freq_1y"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')


# #### 進店區域

# ##### 交易區域和據點對照縣市



lxg_nv_dept_tem = lxg_nv_dept.copy()
lxg_nv_dept_tem['deal_dept'] = lxg_nv_dept_tem['dealercode']+lxg_nv_dept_tem['deptcode']
lxg_nv_dept_tem = lxg_nv_dept_tem[["deal_dept", "contactzipcode", "dept"]]

z_ylg_postcode_tem = z_ylg_postcode.copy()
z_ylg_postcode_tem =z_ylg_postcode_tem[['post_code', 'city' ,'area']]

cs_area_city = lxg_nv_dept_tem.merge(z_ylg_postcode_tem, left_on='contactzipcode', right_on='post_code',how='left')
cs_area_city = cs_area_city[["deal_dept", "contactzipcode", "dept", 'city', 'area']]




# 最近1次
people_offline_df_latest['offline_area_code'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['offline_dlrcode'] + people_offline_df_latest['offline_deptcode'] , None)
cs_area_code_latest = people_offline_df_latest[people_offline_df_latest['offline_area_code'].notnull()]
cs_area_code_latest= cs_area_code_latest.drop_duplicates(['mobile','offline_area_code'], keep='first')
cs_area_code_latest = cs_area_code_latest.merge(cs_area_city, left_on='offline_area_code', right_on='deal_dept',how='left')
cs_area_code_latest = cs_area_code_latest.rename(columns = {'city':'city_latest', 'area':'area_latest','dept':'dept_latest' })
# 最近3個月
people_offline_df_3m['offline_area_code_3m'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['offline_dlrcode'] + people_offline_df_3m['offline_deptcode'] , None)
cs_area_code_3m = people_offline_df_3m[people_offline_df_3m['offline_area_code_3m'].notnull()]
cs_area_code_3m= cs_area_code_3m.drop_duplicates(['mobile','offline_area_code_3m'], keep='first')
cs_area_code_3m = cs_area_code_3m.merge(cs_area_city, left_on='offline_area_code_3m', right_on='deal_dept',how='left')
cs_area_code_3m = cs_area_code_3m.rename(columns = {'city':'city_3m', 'area':'area_3m','dept':'dept_3m'  })
# 最近1年
people_offline_df_oneyear['offline_area_code_1y'] = np.where(people_offline_df_oneyear['day_diff'] >= 0, people_offline_df_oneyear['offline_dlrcode'] + people_offline_df_oneyear['offline_deptcode'] , None)
cs_area_code_1y = people_offline_df_oneyear[people_offline_df_oneyear['offline_area_code_1y'].notnull()]
cs_area_code_1y= cs_area_code_1y.drop_duplicates(['mobile','offline_area_code_1y'], keep='first')
cs_area_code_1y = cs_area_code_1y.merge(cs_area_city, left_on='offline_area_code_1y', right_on='deal_dept',how='left')
cs_area_code_1y = cs_area_code_1y.rename(columns = {'city':'city_1y', 'area':'area_1y','dept':'dept_1y' })



cs_area_code_latest=cs_area_code_latest[cs_area_code_latest.iloc[:, 3:].notnull().any(axis=1)]
cs_area_code_latest = cs_area_code_latest.drop_duplicates(keep='first')
offline_tag1 = offline_tag1.join(cs_area_code_latest.loc[:,["mobile", "city_latest", 'dept_latest' ]].set_index('mobile'), on='mobile')
offline_tag1=offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')

cs_area_code_3m = cs_area_code_3m[cs_area_code_3m.iloc[:, 3:].notnull().any(axis=1)]
cs_area_code_3m = cs_area_code_3m.drop_duplicates(keep='first')
offline_tag1 = offline_tag1.join(cs_area_code_3m.loc[:,["mobile", "city_3m", 'dept_3m']].set_index('mobile'), on='mobile')
offline_tag1=  offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')

cs_area_code_1y = cs_area_code_1y[cs_area_code_1y.iloc[:, 3:].notnull().any(axis=1)]
cs_area_code_1y = cs_area_code_1y.drop_duplicates(keep='first')
offline_tag1 = offline_tag1.join(cs_area_code_1y.loc[:,["mobile", "city_1y", 'dept_1y']].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')


# offline_tag1 = offline_tag1.join(people_offline_df_latest.loc[:,["mobile",  "offline_area_code"]].set_index('mobile'), on='mobile')
# offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
# offline_tag1 = offline_tag1.drop_duplicates(keep='first')

# offline_tag1 = offline_tag1.join(people_offline_df_3m.loc[:,["mobile",  "offline_area_code_3m"]].set_index('mobile'), on='mobile')
# offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
# offline_tag1 = offline_tag1.drop_duplicates(keep='first')

# offline_tag1 = offline_tag1.join(people_offline_df_oneyear.loc[:,["mobile",  "offline_area_code_1y"]].set_index('mobile'), on='mobile')
# offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
# offline_tag1 = offline_tag1.drop_duplicates(keep='first')


# #### 進店週段



# 最近1次
people_offline_df_latest['offline_weekday'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['offline_dayname'] , None)
people_offline_df_latest = people_offline_df_latest[people_offline_df_latest['offline_weekday'].notnull()]
# 最近3個月
people_offline_df_3m['offline_weekday_3m'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['offline_dayname'] , None)
people_offline_df_3m = people_offline_df_3m[people_offline_df_3m['offline_weekday_3m'].notnull()]
# 最近1年
people_offline_df_oneyear['offline_weekday_1y'] = np.where(people_offline_df_oneyear['day_diff'] >= 0, people_offline_df_oneyear['offline_dayname'], None)
people_offline_df_oneyear = people_offline_df_oneyear[people_offline_df_oneyear['offline_weekday_1y'].notnull()]





offline_tag1 = offline_tag1.join(people_offline_df_latest.loc[:,["mobile",  "offline_weekday"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')

offline_tag1 = offline_tag1.join(people_offline_df_3m.loc[:,["mobile",  "offline_weekday_3m"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')

offline_tag1 = offline_tag1.join(people_offline_df_oneyear.loc[:,["mobile",  "offline_weekday_1y"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')


# #### 進店管道



# 最近1次
people_offline_df_latest['offline_channel_f'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['offline_channel'] , None)
people_offline_df_latest = people_offline_df_latest[people_offline_df_latest['offline_channel_f'].notnull()]
# 最近3個月
people_offline_df_3m['offline_channel_3m'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['offline_channel'] , None)
people_offline_df_3m = people_offline_df_3m[people_offline_df_3m['offline_channel_3m'].notnull()]
# 最近1年
people_offline_df_oneyear['offline_channel_1y'] = np.where(people_offline_df_oneyear['day_diff'] >= 0, people_offline_df_oneyear['offline_channel'], None)
people_offline_df_oneyear = people_offline_df_oneyear[people_offline_df_oneyear['offline_channel_1y'].notnull()]




offline_tag1 = offline_tag1.join(people_offline_df_latest.loc[:,["mobile",  "offline_channel_f"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')

offline_tag1 = offline_tag1.join(people_offline_df_3m.loc[:,["mobile",  "offline_channel_3m"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')

offline_tag1 = offline_tag1.join(people_offline_df_oneyear.loc[:,["mobile",  "offline_channel_1y"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')


# #### 距最近1次進店天數



# 最近1次
people_offline_df_latest['offline_latest_interval'] = np.where(people_offline_df_latest['day_diff'] >= 0, people_offline_df_latest['day_diff'], None)
people_offline_df_latest = people_offline_df_latest[people_offline_df_latest['offline_latest_interval'].notnull()]





offline_tag1 = offline_tag1.join(people_offline_df_latest.loc[:,["mobile",  "offline_latest_interval"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')


# #### 進店常為周末或周間


def get_weekend(dayname):
    if (dayname == 'Monday') | (dayname =='Tuesday') | (dayname =='Wednesday') | (dayname =='Thursday') | (dayname =='Friday'):
        return '周間'
    elif (dayname == 'Saturday') | (dayname =='Sunday'):
        return '周末'
    else:
        return  None





# 最近1次
# people_offline_df_3m['offline_weekday'] = np.where(people_offline_df_3m['day_diff'] >= 0, people_offline_df_3m['offline_dayname'], None)

# 最近3個月
people_offline_df_3m['offline_weekend'] = people_offline_df_3m.apply(lambda x: get_weekend(x['offline_dayname']), axis=1)
people_offline_df_3m = people_offline_df_3m[people_offline_df_3m['offline_weekend'].notnull()]
people_offline_df_3m['offline_weekend_yn'] = np.where(people_offline_df_3m['offline_weekend'] == '周末', 1 , 0)
# # 最近1年
people_offline_df_oneyear['offline_weekend'] = people_offline_df_oneyear.apply(lambda x: get_weekend(x['offline_dayname']), axis=1)
people_offline_df_oneyear = people_offline_df_oneyear[people_offline_df_oneyear['offline_weekend'].notnull()]
people_offline_df_oneyear['offline_weekend_yn'] = np.where(people_offline_df_oneyear['offline_weekend'] == '周末', 1 , 0)




# 最近3個月
people_offline_df_3m_weekend= people_offline_df_3m.groupby('mobile')['offline_weekend_yn'].agg(['sum','count','mean'])
people_offline_df_3m_weekend["during_week_3m"] = people_offline_df_3m_weekend['mean'].transform(lambda x: '周末' if (x > 0.6) else '周間')
people_offline_df_3m = people_offline_df_3m.merge(people_offline_df_3m_weekend, left_on='mobile', right_on='mobile',how='left')
# people_offline_df_3m["during_week_3m"] = people_offline_df_3m["offline_dayname"].transform(lambda x: None if pd.isna(x) else people_offline_df_3m["during_week_3m"])

# # 最近1年
people_offline_df_oneyear_weekend= people_offline_df_oneyear.groupby('mobile')['offline_weekend_yn'].agg(['sum','count','mean'])
people_offline_df_oneyear_weekend["during_week_1y"] = people_offline_df_oneyear_weekend['mean'].transform(lambda x: '周末' if (x > 0.6) else '周間')
people_offline_df_oneyear = people_offline_df_oneyear.merge(people_offline_df_oneyear_weekend, left_on='mobile', right_on='mobile',how='left')
# people_crm_df_oneyear["during_week_1y"] = people_crm_df_oneyear["offline_dayname"].transform(lambda x: None if pd.isna(x) else people_crm_df_oneyear["during_week_1y"])





offline_tag1 = offline_tag1.join(people_offline_df_3m.loc[:,["mobile",  "during_week_3m"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')

offline_tag1 = offline_tag1.join(people_offline_df_oneyear.loc[:,["mobile",  "during_week_1y"]].set_index('mobile'), on='mobile')
offline_tag1 = offline_tag1[offline_tag1.iloc[:, 2:].notnull().any(axis=1)]
offline_tag1 = offline_tag1.drop_duplicates(keep='first')


# # offline_tag



tag_names_mapping1 = {
'yl_id' : 'yl_id',
'offline_type_f':'最近一次接觸_需求大項',
'offline_type_3m':'近三個月接觸_需求大項',
'offline_type_1y':'近一年接觸_需求大項',
'offline_item_f': '最近一次接觸_需求項目',
'offline_item_3m':'近三個月接觸_需求項目',
'offline_item_1y':'近一年接觸_需求項目',
'offline_brand_f':'最近一次接觸_需求品牌',
'offline_brand_f':'近三個月接觸_需求品牌',
'offline_brand_f':'近一年接觸_需求品牌',
'cs_carenergy':'最近一次接觸_能源',
'cs_carenergy_3m':'近三個月接觸_能源',
'cs_carenergy_1y':'近一年接觸_能源',
'offline_carmodle_f':'最近一次接觸_車款',
'offline_carmodle_3m':'近三個月接觸_車款',
'offline_carmodle_1y':'近一年接觸_車款',
'cs_carstyle':'最近一次接觸_車身型式',
'cs_carstyle_3m':'近三個月接觸_車身型式',
'cs_carstyle_1y':'近一年接觸_車身型式',
'cs_carseat':'最近一次接觸_乘客數',
'cs_carseat_3m':'近三個月接觸_乘客數',
'cs_carseat_1y':'近一年接觸_乘客數'
}

tmp_table1 = offline_tag.loc[:, list(tag_names_mapping1.keys())].copy()
tmp_table1.rename(columns = tag_names_mapping1, inplace = True)


# # Reshape




tag_table1 = tmp_table1.set_index('yl_id').stack().reset_index(name='value')
tag_table1.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table1['bcat'] = '顧關管理'
tag_table1['mcat'] = tag_table1['tag_name'].transform(lambda x: x.split('_')[0])
tag_table1['scat'] = tag_table1['tag_name'].transform(lambda x: x.split('_')[1])
tag_table1 = tag_table1.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table1['crt_dt'] = datetime.now()
tag_table1['upd_dt'] = datetime.now()
tag_table1['crt_src'] = datetime.now()





tag_names_mapping2 = {
    'yl_id' : 'yl_id',
'offline_crm_type':'最近一次接觸_客服諮詢類型',
'offline_crm_item_3m':'近三個月接觸_客服諮詢類型(Top3)',
'offline_crm_item_1y':'近一年接觸_客服諮詢類型(Top3)',
'crm_freq_3m':'近三個月接觸_客服次數',
'crm_freq_1y':'近一年接觸_客服次數',
'city_latest':'最近一次接觸_進店區域',
'city_3m':'近三個月接觸_進店區域',
'city_1y':'近一年接觸_進店區域',
'offline_weekday':'最近一次接觸_進店週段',
'offline_weekday_3m':'近三個月接觸_進店週段',
'offline_weekday_1y':'近一年接觸_進店週段',
'offline_channel_f':'最近一次接觸_進店管道',
'offline_channel_3m':'近三個月接觸_進店管道',
'offline_channel_1y':'近一年接觸_進店管道',
'offline_latest_interval':'距最近一次接觸_間隔天數',
'during_week_3m':'近三個月接觸_進店常為周末或周間',
'during_week_1y':'近一年接觸_進店常為周末或周間'
}

tmp_table2 = offline_tag1.loc[:, list(tag_names_mapping2.keys())].copy()
tmp_table2.rename(columns = tag_names_mapping2, inplace = True)





tag_table2 = tmp_table2.set_index('yl_id').stack().reset_index(name='value')
tag_table2.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table2['bcat'] = '顧關管理'
tag_table2['mcat'] = tag_table2['tag_name'].transform(lambda x: x.split('_')[0])
tag_table2['scat'] = tag_table2['tag_name'].transform(lambda x: x.split('_')[1])
tag_table2 = tag_table2.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table2['crt_dt'] = datetime.now()
tag_table2['upd_dt'] = datetime.now()
tag_table2['crt_src'] = datetime.now()





tag_table = pd.concat([tag_table1, tag_table2], ignore_index=True)





tag_table = tag_table[tag_table["tag_value"] != '未知']
tag_table = tag_table[tag_table["tag_value"].notnull()]
tag_table = tag_table[tag_table["tag_value"] != '']





brand_tag_define_lxg = brand_tag_define[brand_tag_define['brand_name'] == '納智捷']
tag_table = tag_table.join(brand_tag_define_lxg.loc[:,["tag_name", "tag_id"]].set_index('tag_name'), on='tag_name')





cmain_lxg_people_lxgid = cmain_lxg_people_lxgid[["yl_id", "memid"]]
tag_table = tag_table.join(cmain_lxg_people_lxgid.loc[:,["memid", "yl_id"]].set_index('yl_id'), on='yl_id')





tag_table = tag_table.drop_duplicates(keep='first')
tag_table.rename(columns={'memid':'lxg_id'}, inplace=True )




len(tag_table)





# for i in range(0,len(tag_table),10000):
#     write_into_tag_table(df = tag_table.iloc[i:i+10000], table_name = 'lxg_tag', schema_name = 'dt_tag', method = 'keep')
#     print('The batches are:', '[',  i,  + i+10000, ']', '\n')

