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
    from luxgen.utils.utils.db_conn_info import host_name, database_name, port, user_name, user_pwd
    from luxgen.utils.utils.encryption import TagProjectEncryption

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
SELECT cst_mphone,
cap_desc,
crt_date
FROM xdt_lxg.lxg_nv_opp_cst
where crt_date >= now() - interval '365 day'
;
"""
lxg_nv_opp_cst = doing_query(query_string)



# # web_linemembertag
# query_string = """
# SELECT line_uid,
# tag_name,
# timestamp
# FROM xdt_lxg.lxg_web_linemembertag
# where timestamp >= now() - interval '365 day'
# ;
# """
# lxg_web_linemembertag = doing_query(query_string)

# web_member
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

# n7活動重要性

lxg_n7act_weight = pd.read_excel('./N7活動重要性_1026.xlsx', dtype={'date': date})


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


# ## 互動方式

# #### 有望客資料

# ##### 最近1年


# 計算成為有望客日期和資料更新日期的差幾天(days)
lxg_nv_opp_cst['day_diff'] =  (today - pd.to_datetime(lxg_nv_opp_cst['crt_date']).dt.date)
lxg_nv_opp_cst['day_diff'] =  lxg_nv_opp_cst['day_diff'].dt.days

# 篩選1年內的有望客
lxg_nv_opp_cst_act_oneyear = lxg_nv_opp_cst[(lxg_nv_opp_cst['day_diff'] >= 0 ) & (lxg_nv_opp_cst['day_diff'] <=365) & (lxg_nv_opp_cst['cap_desc'].notnull())]

lxg_nv_opp_cst_act_oneyear['mobile'] = lxg_nv_opp_cst_act_oneyear['cst_mphone']
lxg_nv_opp_cst_act_oneyear['act_item'] = '有望客'
lxg_nv_opp_cst_act_oneyear['act_name'] = lxg_nv_opp_cst_act_oneyear['cap_desc']
lxg_nv_opp_cst_act_oneyear['act_type'] = '線下活動'
lxg_nv_opp_cst_act_oneyear['act_date'] = lxg_nv_opp_cst_act_oneyear['crt_date']
lxg_nv_opp_cst_act_oneyear['act_date_tf'] = lxg_nv_opp_cst_act_oneyear['crt_date'].dt.date

opp_cst_act_oneyear= lxg_nv_opp_cst_act_oneyear[[ 'mobile', 'day_diff','act_item', 'act_name','act_type', 'act_date', 'act_date_tf']]


# #### linetag資料



# # 計算成為有望客日期和資料更新日期的差幾天(days)
# lxg_web_linemembertag['day_diff'] =  (today - pd.to_datetime(lxg_web_linemembertag['timestamp']).dt.date)
# lxg_web_linemembertag['day_diff'] =  lxg_web_linemembertag['day_diff'].dt.days

# # # 篩選1年內的有望客
# lxg_linemembertag_oneyear =lxg_web_linemembertag[(lxg_web_linemembertag['day_diff'] >= 0 ) & (lxg_web_linemembertag['day_diff'] <=365) & (lxg_web_linemembertag['tag_name'].notnull())]

# lxg_linemembertag_oneyear['act_item'] = 'linetag'
# lxg_linemembertag_oneyear['act_name'] = lxg_linemembertag_oneyear['tag_name']
# lxg_linemembertag_oneyear['act_type'] = '線上活動'
# lxg_linemembertag_oneyear['act_date'] = lxg_linemembertag_oneyear['timestamp']
# lxg_linemembertag_oneyear['act_date_tf'] = lxg_linemembertag_oneyear['timestamp'].dt.date

# linemembertag_act_oneyear= lxg_linemembertag_oneyear[[ 'line_uid', 'day_diff','act_item', 'act_name' ,'act_type', 'act_date', 'act_date_tf']]



#  篩選活動 & n7活動
# activity_7m = lxg_n7act_weight

# linemembertag_act_oneyear =linemembertag_act_oneyear[(linemembertag_act_oneyear['act_name'].isin(activity_7m['n7_activities'])) | (linemembertag_act_oneyear['act_name'].str.contains('活動'))]




# distinct 同天同活動
# linemembertag_act_oneyear_dis = linemembertag_act_oneyear.drop_duplicates(['line_uid', 'act_date_tf', 'act_name'], keep='last')
# linemembertag_act_oneyear = linemembertag_act_oneyear_dis


# ##### linemembertag join weblinemember取手機號碼




# linemembertag_act_oneyear = linemembertag_act_oneyear.join(lxg_web_member.loc[:,["line_uid", "phone"]].set_index('line_uid'), on='line_uid')
# linemembertag_act_oneyear = linemembertag_act_oneyear[linemembertag_act_oneyear["phone"].notnull()]





# linemembertag_act_oneyear = linemembertag_act_oneyear.drop(columns='line_uid')
# linemembertag_act_oneyear['mobile']= linemembertag_act_oneyear['phone']
# linemembertag_act_oneyear = linemembertag_act_oneyear.drop(columns='phone')


# #### 併有望客和linemembertag



# 有望客 concat linemembertag
# act_df_oneyear = pd.concat([opp_cst_act_oneyear, linemembertag_act_oneyear], ignore_index=True)
act_df_oneyear = opp_cst_act_oneyear.copy()
# merge人表
people_act_df_oneyear = cmain_lxg_people.merge(act_df_oneyear, left_on='mobile', right_on='mobile',how='left')


# ##### 近3個月資料




people_act_df_3m = people_act_df_oneyear[(people_act_df_oneyear['day_diff'] >= 0) & (people_act_df_oneyear['day_diff'] <= 90)]


# ##### 最近六個月



people_act_df_6m = people_act_df_oneyear[(people_act_df_oneyear['day_diff'] >= 0) & (people_act_df_oneyear['day_diff'] <= 180)]


# ##### 最近1次資料



act_df_oneyear_tm = act_df_oneyear
act_df_oneyear_tm['act_date_tf'] = pd.to_datetime(act_df_oneyear_tm.act_date_tf)
act_df_latest = act_df_oneyear_tm.loc[act_df_oneyear_tm.groupby('mobile')['act_date_tf'].idxmax()]
people_act_df_latest = cmain_lxg_people.merge(act_df_latest, left_on='mobile', right_on='mobile',how='left')


# ### 互動方式標籤表框架


cmain_lxg_people_id = cmain_lxg_people.copy()
mem_col = []
interact_tag = cmain_lxg_people_id.reindex(columns = cmain_lxg_people_id.columns.tolist() + mem_col)


# #### 最近1次參與類型



# 最近1次
people_act_df_latest['act_type_f'] = np.where(people_act_df_latest['day_diff'] >= 0, people_act_df_latest['act_type'], '' )


# #### 最近3個月、6個月和1年最常參與類型



# 最近3個月
people_act_df_3m['act_type_yn'] = np.where(people_act_df_3m['day_diff'] >= 0, 1 , 0)
people_act_df_3m_typesum = people_act_df_3m.groupby(['mobile','act_type'])['act_type_yn'].sum().reset_index(name="act_type_sum_3m")
people_act_df_3m_typesum_f1 = people_act_df_3m_typesum.sort_values('act_type_sum_3m', ascending = False).groupby('mobile').head(1)

# 最近6個月
people_act_df_6m['act_type_yn'] = np.where(people_act_df_6m['day_diff'] >= 0, 1 , 0)
people_act_df_6m_typesum = people_act_df_6m.groupby(['mobile','act_type'])['act_type_yn'].sum().reset_index(name="act_type_sum_6m")
people_act_df_6m_typesum_typesum_f1 = people_act_df_6m_typesum.sort_values('act_type_sum_6m', ascending = False).groupby('mobile').head(1)

# 最近1年
people_act_df_oneyear['act_type_yn'] = np.where(people_act_df_oneyear['day_diff'] >= 0, 1 , 0)
people_act_df_oneyear_typesum = people_act_df_oneyear.groupby(['mobile','act_type'])['act_type_yn'].sum().reset_index(name="act_type_sum_1y")
people_act_df_oneyear_typesum_f1 = people_act_df_oneyear_typesum.sort_values('act_type_sum_1y', ascending = False).groupby('mobile').head(1)





# 最近1次
interact_tag = interact_tag.join(people_act_df_latest.loc[:,["mobile", "act_type_f"]].set_index('mobile'), on='mobile')
interact_tag = interact_tag[interact_tag.iloc[:, 2:].notnull().any(axis=1)]
interact_tag = interact_tag.drop_duplicates(keep='first')

# 最近3個月
interact_tag = interact_tag.join(people_act_df_3m_typesum_f1.loc[:,["mobile", "act_type_sum_3m"]].set_index('mobile'), on='mobile')
interact_tag = interact_tag[interact_tag.iloc[:, 2:].notnull().any(axis=1)]
interact_tag = interact_tag.drop_duplicates(keep='first')

# 最近6個月
interact_tag = interact_tag.join(people_act_df_6m_typesum_typesum_f1.loc[:,["mobile", "act_type_sum_6m"]].set_index('mobile'), on='mobile')
interact_tag = interact_tag[interact_tag.iloc[:, 2:].notnull().any(axis=1)]
interact_tag = interact_tag.drop_duplicates(keep='first')

# 最近1年
interact_tag = interact_tag.join(people_act_df_oneyear_typesum_f1.loc[:,["mobile", "act_type_sum_1y"]].set_index('mobile'), on='mobile')
interact_tag = interact_tag[interact_tag.iloc[:, 2:].notnull().any(axis=1)]
interact_tag = interact_tag.drop_duplicates(keep='first')


# #### 最近3個月、6個月和1年參與活動次數



# 最近3個月次數
people_act_df_3m['act_freq_yn'] = np.where(people_act_df_3m['day_diff'] >= 0, 1 , 0)
people_act_df_3m_tfreq = people_act_df_3m.groupby('mobile')['act_freq_yn'].sum().reset_index(name="act_freq_3m")

# 最近6個月次數
people_act_df_6m['act_freq_yn'] = np.where(people_act_df_6m['day_diff'] >= 0, 1 , 0)
people_act_df_6m_tfreq = people_act_df_6m.groupby('mobile')['act_freq_yn'].sum().reset_index(name="act_freq_6m")

# 最近1年次數
people_act_df_oneyear['act_freq_yn'] = np.where(people_act_df_oneyear['day_diff'] >= 0, 1 , 0)
people_act_df_oneyear_tfreq = people_act_df_oneyear.groupby('mobile')['act_freq_yn'].sum().reset_index(name="act_freq_1y")




# 最近3個月
interact_tag = interact_tag.join(people_act_df_3m_tfreq.loc[:,["mobile", "act_freq_3m"]].set_index('mobile'), on='mobile')
interact_tag = interact_tag[interact_tag.iloc[:, 2:].notnull().any(axis=1)]
interact_tag = interact_tag.drop_duplicates(keep='first')

# 最近6個月
interact_tag = interact_tag.join(people_act_df_6m_tfreq.loc[:,["mobile", "act_freq_6m"]].set_index('mobile'), on='mobile')
interact_tag = interact_tag[interact_tag.iloc[:, 2:].notnull().any(axis=1)]
interact_tag = interact_tag.drop_duplicates(keep='first')

# 最近1年
interact_tag = interact_tag.join(people_act_df_oneyear_tfreq.loc[:,["mobile", "act_freq_1y"]].set_index('mobile'), on='mobile')
interact_tag = interact_tag[interact_tag.iloc[:, 2:].notnull().any(axis=1)]
interact_tag = interact_tag.drop_duplicates(keep='first')


# #### 距最近1次參與活動的天數



# 最近1次
people_act_df_latest['act_latest_interval'] = np.where(people_act_df_latest['day_diff'] >= 0, people_act_df_latest['day_diff'], None)


# In[78]:


# 最近1次
interact_tag = interact_tag.join(people_act_df_latest.loc[:,["mobile", "act_latest_interval"]].set_index('mobile'), on='mobile')
interact_tag = interact_tag[interact_tag.iloc[:, 2:].notnull().any(axis=1)]
interact_tag = interact_tag.drop_duplicates(keep='first')


# # Select



tag_names_mapping = {
    'yl_id': 'yl_id',
    'act_type_f' : '最近一次參與活動_參與類型',
    'act_type_sum_3m' : '近三個月參與活動_最常參與類型',
    'act_type_sum_6m' : '近六個月參與活動_最常參與類型',
    'act_type_sum_1y' : '近一年參與活動_最常參與類型',
    'act_latest_interval' : '距離最近一次參與_活動天數',
    'act_freq_3m' : '近三個月參與活動_參與活動次數',
    'act_freq_6m' : '近六個月參與活動_參與活動次數',
    'act_freq_1y' : '近一年參與活動_參與活動次數'

}

tmp_table = interact_tag.loc[:, list(tag_names_mapping.keys())].copy()
tmp_table.rename(columns = tag_names_mapping, inplace = True)


# # Reshape



tag_table = tmp_table.set_index('yl_id').stack().reset_index(name='value')
tag_table.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
tag_table['bcat'] = '互動方式'
tag_table['mcat'] = tag_table['tag_name'].transform(lambda x: x.split('_')[0])
tag_table['scat'] = tag_table['tag_name'].transform(lambda x: x.split('_')[1])
tag_table = tag_table.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
tag_table['crt_dt'] = datetime.now()
tag_table['upd_dt'] = datetime.now()
tag_table['crt_src'] = datetime.now()




tag_table = tag_table[tag_table["tag_value"] != '未知']
tag_table = tag_table[tag_table["tag_value"].notnull()]
tag_table = tag_table[tag_table["tag_value"] != '']
tag_table = tag_table.drop_duplicates(keep='first')



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

