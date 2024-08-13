#!/usr/bin/env python
# coding: utf-8


from airflow import DAG
from airflow.operators import PythonOperator

import numpy as np
import pandas as pd
import psycopg2
import warnings
warnings.filterwarnings('ignore')
from datetime import date
from datetime import datetime, timedelta
today = date.today()
from utils.helpers import get_age_range, get_years, get_oppstatus, get_regist, get_gender, get_caryears, get_YN, get_YNO, change_language , delete_old_tag, write_into_tag_table,get_customer_contribution

def lxg_membership_task():
    
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
    member_type,
    opp_status,
    is_car_owner,
    is_car_driver,
    line_id,
    registration_time,
    registration_channel,
    birthday,
    gender,
    residence_city,
    residence_district,
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
    info.phone_to_id;
    ;
    """
    cmain_lxg_pii_de = doing_query(query_string)


    # 抓經緯度表(目前只有車主)

    z_lxg_latlon = pd.read_excel('./z_lxg_owner_address_latlon.xlsx', dtype={'master_mobile': str})

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
    bonusexpiry
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
    totalamt
    FROM xdt_lxg.lxg_nv_veh_rbo
    where order_date >= now() - interval '365 day'
    ;
    """
    lxg_nv_veh_rbo = doing_query(query_string)

    # 抓CRM
    query_string = """
    SELECT *
    FROM xdt_lxg.lxg_nv_crm
    where opendate >= now() - interval '365 day'
    ;
    """
    lxg_nv_crm = doing_query(query_string)

    # 抓試駕
    query_string = """
    SELECT *
    FROM xdt_lxg.lxg_nv_testdrive
    where testdrivedate >= now() - interval '365 day'
    ;
    """
    lxg_nv_testdrive = doing_query(query_string)

    # # web_member -- old web
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


    # # web_linemembertag -- no data on the new web
    # query_string = """
    # SELECT line_uid,
    # tag_id,
    # tag_name,
    # timestamp
    # FROM xdt_lxg.lxg_web_linemembertag
    # where ftp_date >= now() - interval '365 day'
    # ;
    # """
    # lxg_web_linemembertag = doing_query(query_string)


    # # dac_website_log -- stop after Jan ,2024
    # query_string = """
    # SELECT *
    # FROM info.dac_website_log
    # where date >= now() - interval '2 day'
    # ;
    # """
    # dac_website_log = doing_query(query_string)

    # # hashed id --no need due to no third party tags
    # query_string = """
    # SELECT *
    # FROM info.hash_map
    # ;
    # """
    # hash_map = doing_query(query_string)

    # 車主居住地離保修廠的距離 lxg_ower_splant_distance

    lxg_ower_splant_distance = pd.read_excel('./data/lxg_ower_splant_distance.xlsx', dtype={'master_mobile': str})

    # n7預購第一波名單-純電代言人

    lxg_n7_order_1st = pd.read_excel('./data/lxg_n7_order_1st_0310.xlsx', dtype={'手機號碼': str})

    # n7預購第二波名單-純電夥伴

    lxg_n7_order_2nd = pd.read_excel('./data/lxg_n7_order_2nd_0310.xlsx', dtype={'手機號碼': str})

    # n7預購退購名單-latest

    lxg_n7_order_cancel_new = pd.read_excel('./data/lxg_n7_order_cancel.xlsx', dtype={'手機號碼': str})

    # n7-NFT盲盒領取開啟
    lxg_n7_nft_list = pd.read_excel('./data/lxg_n7_nft_list_202401160014.xlsx', dtype={'uuid': str})

    # n7活動重要性
    lxg_n7act_weight = pd.read_excel('./data/N7活動重要性_1026.xlsx', dtype={'date': date})

    # brand_tag_define
    query_string = """
    SELECT *
    FROM dt_tag.brand_tag_define
    ;
    """
    brand_tag_define = doing_query(query_string)

    # 行冠人表資料

    query_string = """
    SELECT 
    aa.sso_id,
    aa.pii_token,
    bb.phone
    FROM info.cmain_mlf_people aa
    left join info.phone_to_id bb
    on aa.pii_token = bb.pii_token
    ;
    """
    cmain_mlf_people = doing_query(query_string)

    # 格上人表資料

    query_string = """
    SELECT 
    aa.sso_id,
    aa.pii_token,
    bb.phone,
    convert_from(decrypt(decode(bb.phone, 'base64'),'evm$9xyyBar%8ZpRSMNdQEgwZrdY8qFx','aes-ecb'), 'UTF8') AS de_columnname
    FROM info.cmain_cpt_people aa
    left join info.phone_to_id bb
    on aa.pii_token = bb.pii_token
    ;
    """
    cmain_cpt_people = doing_query(query_string)


    # ## Transform


    cmain_lxg_pii_de.rename(columns={'phone': 'mobile'}, inplace=True)
    cmain_lxg_people = cmain_lxg_people.merge(cmain_lxg_pii_de, left_on='pii_token', right_on='pii_token',how='left')



    lxg_nweb_user_members.rename(columns={'luxgenuserid': 'uuid', 'lineuserid': 'line_uid', }, inplace=True)
    lxg_nweb_user_members = lxg_nweb_user_members.merge(lxg_nweb_user_member_infos, left_on='memid', right_on='memid',how='left')
    cmain_lxg_pii_de_phone = cmain_lxg_pii_de[['mobile','de_columnname']]
    lxg_nweb_user_members = lxg_nweb_user_members.merge(cmain_lxg_pii_de_phone, left_on='phone_09', right_on='mobile',how='left')

    lxg_web_member = lxg_nweb_user_members.copy()


    cmain_lxg_people_lxgid = cmain_lxg_people[['yl_id', 'sso_id' ,'mobile']]
    lxg_nweb_user_members_memid = lxg_nweb_user_members[['memid' ,'mobile']]
    cmain_lxg_people_lxgid = cmain_lxg_people_lxgid.merge(lxg_nweb_user_members_memid, left_on='mobile', right_on='mobile',how='left')


    # ## 會員屬性

    # ### 會員屬性標籤表框架1



    cmain_lxg_people_id = cmain_lxg_people[['yl_id', 'sso_id' ,'mobile', 'de_columnname']]
    mem_col = []
    cmain_lxg_people_membertag = cmain_lxg_people_id.reindex(columns = cmain_lxg_people_id.columns.tolist() + mem_col)


    # ### 會員屬性標籤表框架2

    cmain_lxg_people_id = cmain_lxg_people[['yl_id', 'sso_id' ,'mobile', 'de_columnname']]
    mem_col = []
    cmain_lxg_people_membertag2 = cmain_lxg_people_id.reindex(columns = cmain_lxg_people_id.columns.tolist() + mem_col)




    # 聯繫方式_是否有電子郵件
    # cmain_lxg_people_membertag['is_email'] = cmain_lxg_people['email'].isnull().transform(lambda x: int(x))
    # 年齡
    cmain_lxg_people_membertag['age'] = (today - cmain_lxg_people['birthday']).transform(get_years)
    # # 性別
    cmain_lxg_people_membertag['gender'] = cmain_lxg_people['gender'].transform(get_gender)
    # 名下是否有車
    cmain_lxg_people_membertag['is_owncar'] = cmain_lxg_people['is_car_owner'].transform(lambda x: '是' if  x == 1 else '否')
    cmain_lxg_people_membertag['is_lxg_member'] = cmain_lxg_people['member_type'].transform(lambda x: '是' if  x == 1 else '否')
    cmain_lxg_people_membertag['opp_status'] = cmain_lxg_people['opp_status'].transform(get_oppstatus)
    cmain_lxg_people_membertag['is_lxg_car_owner'] = cmain_lxg_people['is_car_owner'].transform(get_YNO)
    cmain_lxg_people_membertag['is_lxg_car_driver'] = cmain_lxg_people['is_car_driver'].transform(get_YNO)
    cmain_lxg_people_membertag['is_line_id'] = cmain_lxg_people['line_id'].transform(lambda x: '是' if  pd.notnull(x) else '否')
    cmain_lxg_people_membertag['registration_channel'] = cmain_lxg_people['registration_channel'].transform(get_regist)
    # 距註冊天數
    cmain_lxg_people_membertag['registration_time'] = (today - pd.to_datetime(cmain_lxg_people['registration_time']).dt.date).transform(lambda x: x.days if (x is not None) else None)

    cmain_lxg_people_membertag['residence_city'] = cmain_lxg_people['residence_city']
    cmain_lxg_people_membertag['residence_district'] = cmain_lxg_people['residence_district']
    # 距第一次有望客天數
    cmain_lxg_people_membertag['lxg_first_opp_time'] = (today - pd.to_datetime(cmain_lxg_people['first_opp_time']).dt.date).transform(lambda x: x.days if (x is not None) else None)
    # 距最近一次有望客天數
    cmain_lxg_people_membertag['lxg_last_opp_time'] = (today - pd.to_datetime(cmain_lxg_people['last_opp_time']).dt.date).transform(lambda x: x.days if (x is not None) else None)
    # 距第一次試駕天數
    cmain_lxg_people_membertag['lxg_first_test_drive_time'] = (today - pd.to_datetime(cmain_lxg_people['first_test_drive_time']).dt.date).transform(lambda x: x.days if (x is not None) else None)
    # 距最近一次試駕天數
    cmain_lxg_people_membertag['lxg_last_test_drive_time'] = (today - pd.to_datetime(cmain_lxg_people['last_test_drive_time']).dt.date).transform(lambda x: x.days if (x is not None) else None)
    # 距第一次購車天數
    cmain_lxg_people_membertag['lxg_first_buy_time'] = (today - pd.to_datetime(cmain_lxg_people['first_buy_time']).dt.date).transform(lambda x: x.days if (x is not None) else None)
    # 距最近一次購車天數
    cmain_lxg_people_membertag['lxg_last_buy_time'] = (today - pd.to_datetime(cmain_lxg_people['last_buy_time']).dt.date).transform(lambda x: x.days if (x is not None) else None)
    # 距第一次交車天數
    cmain_lxg_people_membertag['lxg_first_deliver_time'] = (today - pd.to_datetime(cmain_lxg_people['first_deliver_time']).dt.date).transform(lambda x: x.days if (x is not None) else None)
    # 距最近一次交車天數
    cmain_lxg_people_membertag['lxg_last_deliver_time'] = (today - pd.to_datetime(cmain_lxg_people['last_deliver_time']).dt.date).transform(lambda x: x.days if (x is not None) else None)




    # 年齡區間
    cmain_lxg_people_membertag['age_range'] = cmain_lxg_people_membertag['age'].transform(get_age_range)



    # 是否為過保固車(最近一部車車齡)
    lxg_nv_vehicle_latest = lxg_nv_vehicle.sort_values(['deliverydate']).drop_duplicates('ownermobile_09', keep='last')


    lxg_nv_vehicle_latest['is_warrantyexpiry'] = pd.to_datetime(lxg_nv_vehicle_latest['warrantyexpiry']).dt.date.transform(lambda x: 1 if  x <= today else 0)
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(lxg_nv_vehicle_latest.loc[:,["ownermobile_09", "is_warrantyexpiry"]].set_index('ownermobile_09'), on='mobile')
    cmain_lxg_people_membertag['is_warrantyexpiry'] = cmain_lxg_people_membertag['is_warrantyexpiry'].transform(get_YN)


    # 是否為過保險車(最近一部車車齡)
    lxg_nv_vehicle_latest['is_insuranceexpiry'] = pd.to_datetime(lxg_nv_vehicle_latest['insuranceexpiry']).dt.date.transform(lambda x: 1 if  x <= today else 0)
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(lxg_nv_vehicle_latest.loc[:,["ownermobile_09", "is_insuranceexpiry"]].set_index('ownermobile_09'), on='mobile')
    cmain_lxg_people_membertag['is_insuranceexpiry'] = cmain_lxg_people_membertag['is_insuranceexpiry'].transform(get_YN)



    cmain_lxg_people_membertag=cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')



    # 車齡(最近一部車車齡)
    lxg_nv_vehicle_latest['carage'] = (today - pd.to_datetime(lxg_nv_vehicle_latest['deliverydate']).dt.date).transform(get_caryears)
    lxg_nv_vehicle_tem = lxg_nv_vehicle_latest[['ownermobile_09','carage']]
    cmain_lxg_people_membertag2 = cmain_lxg_people_membertag2.merge(lxg_nv_vehicle_tem, left_on='mobile', right_on='ownermobile_09',how='left')


    # ##### 車齡分類



    def carage_cat(carage_cat):
        if (carage_cat <= 5) & (np.isnan(carage_cat)):
            return '5年內新車'
        elif (carage_cat > 5) & (carage_cat <= 10):
            return '5-10年車'   
        elif (carage_cat > 10):
            return '10年以上車'         
        else:
            return None



    cmain_lxg_people_membertag2['carage_cat'] = cmain_lxg_people_membertag2.apply(lambda x: carage_cat(x['carage']), axis=1)


    cmain_lxg_people_membertag2=cmain_lxg_people_membertag2[cmain_lxg_people_membertag2.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag2 = cmain_lxg_people_membertag2.drop_duplicates(keep='first')




    # 是否還有紅利點數(最近一部車車齡)
    lxg_nv_vehicle_latest['is_bonus'] = lxg_nv_vehicle_latest['bonus'].transform(lambda x: 1 if  x >= 0 else 0)
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(lxg_nv_vehicle_latest.loc[:,["ownermobile_09", "is_bonus"]].set_index('ownermobile_09'), on='mobile')
    cmain_lxg_people_membertag['is_bonus'] = cmain_lxg_people_membertag['is_bonus'].transform(get_YN)



    # # 紅利點數是否到期(最近一部車車齡)
    lxg_nv_vehicle_latest['is_bonusexpiry'] = lxg_nv_vehicle_latest['bonusexpiry'].transform(lambda x: 1 if ((x is not None) and (x <= today)) else  0 if ((x is not None) and (x > today)) else  None)
    cmain_lxg_people_membertag2 = cmain_lxg_people_membertag2.join(lxg_nv_vehicle_latest.loc[:,["ownermobile_09", "is_bonusexpiry"]].set_index('ownermobile_09'), on='mobile')
    cmain_lxg_people_membertag2['is_bonusexpiry'] = cmain_lxg_people_membertag2['is_bonusexpiry'].transform(get_YN)



    cmain_lxg_people_membertag2=cmain_lxg_people_membertag2[cmain_lxg_people_membertag2.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag2 = cmain_lxg_people_membertag2.drop_duplicates(keep='first')


    # #### 距最近一次進廠天數 

    lxg_nv_svcro['rono_10'] = lxg_nv_svcro['rono'].str.slice(0, 10)
    return_lasttime_b = lxg_nv_svcro.groupby('regono')['trandate'].transform(max) == lxg_nv_svcro['trandate']
    return_lasttime = lxg_nv_svcro[return_lasttime_b][['regono', 'odoreading', 'trandate']]

    # merge 車籍表 取得手機號碼 by regono
    lxg_nv_vehicle_maxtdt = lxg_nv_vehicle.merge(return_lasttime, left_on='regono', right_on='regono',how='left')
    lxg_nv_vehicle_maxtdt['trandate'] = pd.to_datetime(lxg_nv_vehicle_maxtdt.trandate, format='%Y-%m-%d')
    lxg_nv_vehicle_maxtdt_b = lxg_nv_vehicle_maxtdt.groupby('ownermobile_09')['trandate'].transform(max) == lxg_nv_vehicle_maxtdt['trandate']

    # 每位車主最近一次回廠日期 若車主有多台車，抓所擁有車中最近一次回廠日
    lxg_nv_vehicle_maxtdt_tem = lxg_nv_vehicle_maxtdt[lxg_nv_vehicle_maxtdt_b]

    # 計算最近一次進廠日期離資料更新日之天數
    lxg_nv_vehicle_maxtdt_tem['returned_days'] = (today - pd.to_datetime(lxg_nv_vehicle_maxtdt_tem['trandate']).dt.date).transform(lambda x: x.days if (x is not None) else None)

    # merge人表
    # cmain_lxg_people = cmain_lxg_people.merge(lxg_nv_vehicle_maxtdt_tem, left_on='mobile', right_on='ownermobile',how='left')
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(lxg_nv_vehicle_maxtdt_tem.loc[:,["ownermobile_09", "returned_days"]].set_index('ownermobile_09'), on='mobile')


    cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # #### 擁車數量



    # 擁車數量 
    car_amount=lxg_nv_vehicle.groupby('ownermobile_09')['ownermobile_09'].count().reset_index(name="car_count")
    lxg_nv_vehicle_caramount = lxg_nv_vehicle.merge(car_amount, left_on='ownermobile_09', right_on='ownermobile_09',how='left')
    cmain_lxg_people_membertag2 = cmain_lxg_people_membertag2.join(lxg_nv_vehicle_caramount.loc[:,["ownermobile_09", "car_count"]].set_index('ownermobile_09'), on='mobile')



    cmain_lxg_people_membertag2=cmain_lxg_people_membertag2[cmain_lxg_people_membertag2.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag2 = cmain_lxg_people_membertag2.drop_duplicates(keep='first')


    # #### 重複購車


    # 重複購車
    lxg_nv_vehicle_caramount['rep_buy'] = lxg_nv_vehicle_caramount['car_count'].transform(lambda x: 1 if x > 1 else 0)
    cmain_lxg_people_membertag2 = cmain_lxg_people_membertag2.join(lxg_nv_vehicle_caramount.loc[:,["ownermobile_09", "rep_buy"]].set_index('ownermobile_09'), on='mobile')



    cmain_lxg_people_membertag2=cmain_lxg_people_membertag2[cmain_lxg_people_membertag2.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag2 = cmain_lxg_people_membertag2.drop_duplicates(keep='first')


    # ### 訂單資料

    # ##### 最近1年



    # 篩有效的訂單
    lxg_nv_veh_rbo_eft = lxg_nv_veh_rbo[lxg_nv_veh_rbo['sts_desc'].isin(['受訂',  '結帳', '配車',  '領牌', '交車核准', '交車'])]

    # 計算訂單日期和資料更新日期的差幾天(days)
    lxg_nv_veh_rbo_eft['day_diff'] =  (today - pd.to_datetime(lxg_nv_veh_rbo_eft['order_date']).dt.date)
    lxg_nv_veh_rbo_eft['day_diff'] =  lxg_nv_veh_rbo_eft['day_diff'].dt.days

    # 篩1年內的訂單
    lxg_nv_veh_rbo_eft_oneyear = lxg_nv_veh_rbo_eft[(lxg_nv_veh_rbo_eft['day_diff'] >= 0 ) & (lxg_nv_veh_rbo_eft['day_diff'] <=365)]
    lxg_nv_veh_rbo_eft_oneyear['cs_freq_1y'] = 1
    lxg_nv_veh_rbo_eft_oneyear['cost_1y'] = lxg_nv_veh_rbo_eft_oneyear['totalamt']

    lxg_nv_veh_rbo_eft_ctb = lxg_nv_veh_rbo_eft_oneyear[["rego_no",  "cs_freq_1y", "cost_1y"]]


    # ### 工單資料

    # ##### 最近1年

    # 篩有效的保修單 
    lxg_nv_svcro_eft = lxg_nv_svcro[lxg_nv_svcro['status'].isin(['結帳'])]
    lxg_nv_svcro_eft['rono_10'] = lxg_nv_svcro_eft['rono'].str.slice(0, 10)

    # 計算保修單日期和資料更新日期的差幾天(days)
    lxg_nv_svcro_eft['day_diff'] =  (today - pd.to_datetime(lxg_nv_svcro_eft['trandate']).dt.date)
    lxg_nv_svcro_eft['day_diff'] =  lxg_nv_svcro_eft['day_diff'].dt.days

    # 篩1年內保修單
    lxg_nv_svcro_eft_oneyear = lxg_nv_svcro_eft[(lxg_nv_svcro_eft['day_diff'] >= 0) & (lxg_nv_svcro_eft['day_diff'] <= 365)]
    # 計算每輛車1年內總保修金額
    lxg_nv_svcro_eft_oneyear_tsell = lxg_nv_svcro_eft_oneyear.groupby('regono')['totalsell'].sum().reset_index(name="cost_1y")

    # 計算每輛車1年內回廠次數

    # -先去除同廠同工單編號
    lxg_nv_svcro_eft_oneyear_tfreq_tem = lxg_nv_svcro_eft_oneyear.drop_duplicates(['dlrcode', 'deptcode', 'regono','rono_10'], keep='last')
    # 計算回廠次數
    lxg_nv_svcro_eft_oneyear_tfreq = lxg_nv_svcro_eft_oneyear_tfreq_tem.groupby('regono')['regono'].count().reset_index(name="cs_freq_1y")


    # #### 併最近1年的訂單&工單

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

    #  人表 join 工單
    # 未剔除法人和二手車商
    cmain_lxg_people_svcro_ctb = cmain_lxg_people.merge(lxg_svcro_ctb, left_on='mobile', right_on='ownermobile_09',how='left')
    # cmain_lxg_people_ctb.rename(columns = {'day_diff_x':'day_diff_order', 'day_diff_y':'day_diff_svcro'}, inplace = True)

    # concat 訂單
    cmain_lxg_people_ctb = pd.concat([cmain_lxg_people_rbo_ctb,cmain_lxg_people_svcro_ctb], ignore_index=True)

    # 剔除法人和二手車商
    # cmain_lxg_people_nooffical = cmain_lxg_people[cmain_lxg_people["gender"] != '未知']
    # cmain_lxg_people_ctb = cmain_lxg_people_nooffical.merge(lxg_nv_veh_rbo_ctb8, left_on='mobile', right_on='ownermobile',how='left')


    # #### 客戶貢獻

    # def get_lxg_customer_contribution(amount):
    #     if (amount == 0) | (np.isnan(amount)):
    #         return '零貢獻'
    #     elif amount < Q1:
    #         return '低貢獻'
    #     elif amount < Q3:
    #         return '中貢獻'
    #     else:
    #         return '高貢獻'

    # 5.會員貢獻度
    # 區分成零貢獻、低貢獻、中貢獻和高貢獻，4類
    def get_customer_contribution(amount, lowv, medv):
        bins = [-1, 0, lowv, medv, np.inf]
        names = ['零貢獻', '低貢獻', '中貢獻', '高貢獻']
        return pd.cut(amount, bins, labels=names)
    # amount:欲分析之金額欄位
    # medv: 中貢獻金額
    # lowv:低貢獻金額




    # 計算1年內每位會員消費金額
    cmain_lxg_people_ctb_tsell = cmain_lxg_people_ctb.groupby('mobile')['cost_1y'].sum().reset_index(name="tcost_1y")
    Q1 = cmain_lxg_people_ctb_tsell[cmain_lxg_people_ctb_tsell['tcost_1y'] != 0].quantile(0.25).astype('int').item()
    Q3 = cmain_lxg_people_ctb_tsell[cmain_lxg_people_ctb_tsell['tcost_1y'] != 0].quantile(0.75).astype('int').item()
    # cmain_lxg_people_ctb_tsell['contribution'] = cmain_lxg_people_ctb_tsell['tcost_1y'].transform(get_customer_contribution)
    cmain_lxg_people_ctb_tsell['contribution'] = get_customer_contribution(cmain_lxg_people_ctb_tsell['tcost_1y'], Q1, Q3)




    cmain_lxg_people = cmain_lxg_people.join(cmain_lxg_people_ctb_tsell.loc[:,["mobile", "contribution"]].set_index('mobile'), on='mobile')
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(cmain_lxg_people_ctb_tsell.loc[:,["mobile", "contribution"]].set_index('mobile'), on='mobile')




    cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # #### 客戶忠誠



    # def get_lxg_customer_loyalty(amount, csfreq):
    #     if (csfreq == 0) | (np.isnan(csfreq)):
    #         return '零'
    #     elif ((amount == 0) | (np.isnan(amount))) & ((csfreq != 0) & ~np.isnan(csfreq)) :
    #         return '低'
    #     elif (amount < Q1) & (csfreq < fQ1):
    #         return '次低'
    #     elif (amount < Q1) & (csfreq < fQ3):
    #         return '中'
    #     elif (amount < Q3) & (csfreq < fQ3):
    #         return '次高'
    #     else:
    #         return '高'

    # 5.會員忠誠度
    def get_customer_loyalty(df, amount, times):
        df['amount_q'] = pd.qcut(df[amount], 2, labels = False, duplicates='drop') # 交易金額以四分位作切分標準
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




    # # 計算1年內每位會員消費金額
    cmain_lxg_people_ctb_tsell = cmain_lxg_people_ctb.groupby('mobile')['cost_1y'].sum().reset_index(name="tcost_1y")
    cmain_lxg_people_ctb_tfreq = cmain_lxg_people_ctb.groupby('mobile')['cs_freq_1y'].sum().reset_index(name="cs_tfreq_1y")
    cmain_lxg_people_ctb_loyalty = cmain_lxg_people_ctb_tsell.join(cmain_lxg_people_ctb_tfreq.loc[:,["mobile", "cs_tfreq_1y"]].set_index('mobile'), on='mobile')

    # Q1 = cmain_lxg_people_ctb_loyalty[cmain_lxg_people_ctb_loyalty['tcost_1y'] != 0].quantile(0.25).astype('int').item()
    # Q3 = cmain_lxg_people_ctb_loyalty[cmain_lxg_people_ctb_loyalty['tcost_1y'] != 0].quantile(0.75).astype('int').item()
    # fQ1 = cmain_lxg_people_ctb_loyalty[cmain_lxg_people_ctb_loyalty['cs_tfreq_1y'] != 0].quantile(0.25).astype('int').item()
    # fQ3 = cmain_lxg_people_ctb_loyalty[cmain_lxg_people_ctb_loyalty['cs_tfreq_1y'] != 0].quantile(0.75).astype('int').item()

    fQ3 = cmain_lxg_people_ctb_loyalty[cmain_lxg_people_ctb_loyalty['cs_tfreq_1y'] != 0]

    cmain_lxg_people_ctb_loyalty['loyalty'] = get_customer_loyalty(cmain_lxg_people_ctb_loyalty, 'tcost_1y', 'cs_tfreq_1y')

    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(cmain_lxg_people_ctb_loyalty.loc[:,["mobile", "loyalty"]].set_index('mobile'), on='mobile')




    cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # #### 車主價值



    def car_owner_value(meprice, csfreq):
        if (meprice == 0) & (csfreq <= 1):
            return '停滯客戶'
        elif (meprice == 0) & ((csfreq > 1) & (csfreq <= 3)):
            return '停滯客戶'
        elif (meprice == 0) & (csfreq > 3):
            return '停滯客戶'   
        elif ((meprice > 0) & (meprice <= 6000)) & (csfreq <= 1) :
            return '安靜車主'
        elif ((meprice > 0) & (meprice <= 6000)) & ((csfreq > 1) & (csfreq <= 3)) :
            return '精打細算新客'
        elif ((meprice > 0) & (meprice <= 6000)) & (csfreq > 3) :
            return '忠誠新客'
        elif ((meprice > 6000) & (meprice <= 10000)) & (csfreq <= 1) :
            return '游離客'
        elif ((meprice > 6000) & (meprice <= 10000)) & ((csfreq > 1) & (csfreq <= 3)) :
            return '規律平實新客'
        elif ((meprice > 6000) & (meprice <= 10000)) & (csfreq > 3) :
            return '高頻次忠誠'
        elif (meprice > 10000) & (csfreq <= 1) :
            return '回娘家老客'
        elif (meprice > 10000) & ((csfreq > 1) & (csfreq <= 3)) :
            return '老實舊客'
        elif (meprice > 10000) & (csfreq > 3) :
            return '高貢獻忠誠'          
        else:
            return '未知'




    cmain_lxg_people_ctb_value = cmain_lxg_people_ctb_tsell.join(cmain_lxg_people_ctb_tfreq.loc[:,["mobile", "cs_tfreq_1y"]].set_index('mobile'), on='mobile')
    cmain_lxg_people_ctb_value['meprice'] = cmain_lxg_people_ctb_value['tcost_1y']/cmain_lxg_people_ctb_value['cs_tfreq_1y']
    cmain_lxg_people_ctb_value['value_class'] = cmain_lxg_people_ctb_value.apply(lambda x: car_owner_value(x['meprice'], x['cs_tfreq_1y']), axis=1)




    cmain_lxg_people = cmain_lxg_people.join(cmain_lxg_people_ctb_value.loc[:,["mobile", "meprice","value_class"]].set_index('mobile'), on='mobile')
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(cmain_lxg_people_ctb_value.loc[:,["mobile", "value_class"]].set_index('mobile'), on='mobile')


    # #### 用車程度



    # 最近1年
    lxg_nv_svcro_odo_tem1 = lxg_nv_svcro_eft_oneyear[['dlrcode', 'dlr', 'deptcode', 'dept', 'rono_10' ,'regono', 'trandate', 'odoreading']]
    # 移除重複-同廠同工單編號
    lxg_nv_svcro_odo_tem2 = lxg_nv_svcro_odo_tem1.drop_duplicates(['dlrcode', 'deptcode', 'regono','rono_10'])
    # # 依回廠日期排序
    # lxg_nv_svcro_odo_tem3 = lxg_nv_svcro_odo_tem2.sort_values('trandate', ascending=False).groupby('regono').head(3).reset_index(drop=True)
    # # 計算每次回廠間的行駛里程數和天數
    # lxg_nv_svcro_odo_tem3['lag_odoreading'] = lxg_nv_svcro_odo_tem3.sort_values('trandate', ascending=False).groupby('regono')['odoreading'].shift(1)
    # lxg_nv_svcro_odo_tem3['lag_trandate'] = lxg_nv_svcro_odo_tem3.sort_values('trandate', ascending=False).groupby('regono')['trandate'].shift(1)
    # lxg_nv_svcro_odo_tem3['trandate_interval'] = (lxg_nv_svcro_odo_tem3['lag_trandate'] - lxg_nv_svcro_odo_tem3['trandate']).dt.days
    # lxg_nv_svcro_odo_tem3['odoreading_interval'] = (lxg_nv_svcro_odo_tem3['lag_odoreading'] - lxg_nv_svcro_odo_tem3['odoreading'])
    # lxg_nv_svcro_odo_tem3['odoreading_per_day'] = lxg_nv_svcro_odo_tem3['odoreading_interval']/lxg_nv_svcro_odo_tem3['trandate_interval']
    # # 計算近三次回廠平均每天行駛里程數
    # lxg_nv_svcro_odo_tem4 = lxg_nv_svcro_odo_tem3.groupby('regono')['odoreading_per_day'].mean().reset_index(name="odoreading_mean")

    # # 依回廠日期抓最大最小相減
    lxg_nv_svcro_odo_tem3_max = lxg_nv_svcro_odo_tem2.groupby('regono').max()
    lxg_nv_svcro_odo_tem3_max = lxg_nv_svcro_odo_tem3_max.reset_index()
    lxg_nv_svcro_odo_tem3_min = lxg_nv_svcro_odo_tem2.groupby('regono').min()
    lxg_nv_svcro_odo_tem3_min = lxg_nv_svcro_odo_tem3_min.reset_index()

    lxg_nv_svcro_odo_tem3_max = lxg_nv_svcro_odo_tem3_max[['regono', 'trandate', 'odoreading']]
    lxg_nv_svcro_odo_tem3_max = lxg_nv_svcro_odo_tem3_max.rename(columns={'trandate':'trandate_max', 'odoreading' : 'odoreading_max'})
    lxg_nv_svcro_odo_tem3_min = lxg_nv_svcro_odo_tem3_min[['regono', 'trandate', 'odoreading']]
    lxg_nv_svcro_odo_tem3_min = lxg_nv_svcro_odo_tem3_min.rename(columns={'trandate':'trandate_min', 'odoreading' : 'odoreading_min'})
    lxg_nv_svcro_odo_tem3 = lxg_nv_svcro_odo_tem3_max.merge(lxg_nv_svcro_odo_tem3_min, left_on='regono', right_on='regono', how='left')
    lxg_nv_svcro_odo_tem3['trandate_interval'] = (lxg_nv_svcro_odo_tem3['trandate_max'] - lxg_nv_svcro_odo_tem3['trandate_min']).dt.days
    lxg_nv_svcro_odo_tem3['odoreading_interval'] = (lxg_nv_svcro_odo_tem3['odoreading_max'] - lxg_nv_svcro_odo_tem3['odoreading_min'])
    lxg_nv_svcro_odo_tem3['odoreading_per_day'] = lxg_nv_svcro_odo_tem3['odoreading_interval']/lxg_nv_svcro_odo_tem3['trandate_interval']

    lxg_nv_svcro_odo_tem4 = lxg_nv_svcro_odo_tem3.groupby('regono')['odoreading_per_day'].mean().reset_index(name="odoreading_mean")

    # # 依回廠日期排序前三次
    # # 計算每次回廠間的行駛里程數和天數
    # lxg_nv_svcro_odo_tem3['lag_odoreading'] = lxg_nv_svcro_odo_tem3.sort_values('trandate', ascending=False).groupby('regono')['odoreading'].shift(1)
    # lxg_nv_svcro_odo_tem3['lag_trandate'] = lxg_nv_svcro_odo_tem3.sort_values('trandate', ascending=False).groupby('regono')['trandate'].shift(1)
    # lxg_nv_svcro_odo_tem3['trandate_interval'] = (lxg_nv_svcro_odo_tem3['lag_trandate'] - lxg_nv_svcro_odo_tem3['trandate']).dt.days
    # lxg_nv_svcro_odo_tem3['odoreading_interval'] = (lxg_nv_svcro_odo_tem3['lag_odoreading'] - lxg_nv_svcro_odo_tem3['odoreading'])
    # lxg_nv_svcro_odo_tem3['odoreading_per_day'] = lxg_nv_svcro_odo_tem3['odoreading_interval']/lxg_nv_svcro_odo_tem3['trandate_interval']
    # # 計算近三次回廠平均每天行駛里程數
    # lxg_nv_svcro_odo_tem4 = lxg_nv_svcro_odo_tem3.groupby('regono')['odoreading_per_day'].mean().reset_index(name="odoreading_mean")





    lxg_nv_vehicle_odo = lxg_nv_vehicle[["regono", "ownermobile_09"]]
    # join車籍表&人表
    lxg_nv_svcro_odo_tem5 = lxg_nv_svcro_odo_tem4.merge(lxg_nv_vehicle_odo, left_on='regono', right_on='regono', how='left')
    cmain_lxg_people_odo_mobile = cmain_lxg_people[["mobile"]]
    cmain_lxg_people_odo = cmain_lxg_people_odo_mobile.merge(lxg_nv_svcro_odo_tem5, left_on='mobile', right_on='ownermobile_09',how='left')

    # 可能有多台車，再計算每人平均里程
    cmain_lxg_people_odo1 = cmain_lxg_people_odo.groupby('mobile')['odoreading_mean'].mean().reset_index(name="odoreading_mean_final")
    owner_perday_odoreading = cmain_lxg_people_odo_mobile.join(cmain_lxg_people_odo1.loc[:,["mobile", "odoreading_mean_final"]].set_index('mobile'), on='mobile')


    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(owner_perday_odoreading.loc[:,["mobile", "odoreading_mean_final"]].set_index('mobile'), on='mobile')

    cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # ##### 用車用途

    def odo_purpose(odo):
        if (odo <= 58) & (np.isnan(odo)):
            return '購物'
        elif (odo > 58) & (odo <= 74):
            return '通勤'
        elif (odo > 74) & (odo <= 80):
            return '探視或接送親人、小孩'
        elif (odo > 80) & (odo <= 100):
            return '洽公或業務使用'    
        elif (odo > 100):
            return '休閒'         
        else:
            return None



    cmain_lxg_people_membertag['odo_purpose'] = cmain_lxg_people_membertag.apply(lambda x: odo_purpose(x['odoreading_mean_final']), axis=1)


    # #### 購車熟度計算

    # ##### 最近1台車

    # 最近1台車的車齡(交車時間)
    lxg_nv_vehicle_change = lxg_nv_vehicle[["ownermobile_09", "deliverydate"]]
    cmain_lxg_people_change = cmain_lxg_people[["mobile"]]
    vehicle_change = cmain_lxg_people_change.merge(lxg_nv_vehicle_change, left_on='mobile', right_on='ownermobile_09',how='left')
    vehicle_change_latest = vehicle_change.loc[vehicle_change[vehicle_change['deliverydate'].notnull()].groupby('mobile')['deliverydate'].idxmax()]


    # ##### 換車間隔

    #  二手車商 0911234565 delete
    lxg_nv_vehicle_change1 = lxg_nv_vehicle_change.copy()
    cmain_lxg_people_change = cmain_lxg_people[["mobile", "gender", "value_class"]]
    cmain_lxg_people_change = cmain_lxg_people_change[(cmain_lxg_people_change["gender"] != "未知") ]
    cmain_lxg_people_change = cmain_lxg_people_change[(cmain_lxg_people_change["mobile"] != "0911234565")]
    vehicle_change_intval = cmain_lxg_people_change.merge(lxg_nv_vehicle_change1, left_on='mobile', right_on='ownermobile_09',how='left')
    vehicle_change_intval_tem1 = vehicle_change_intval.drop_duplicates(['mobile', 'deliverydate'])
    vehicle_change_intval_tem1['lag_deliverydate']= (vehicle_change_intval_tem1.sort_values(by=['deliverydate'],ascending=True)).groupby(['mobile'])['deliverydate'].shift(1)
    vehicle_change_intval_tem1['buycar_interval'] = (vehicle_change_intval_tem1['deliverydate'] - vehicle_change_intval_tem1['lag_deliverydate']).dt.days

    # -自己的換車平均間隔
    vehicle_change_intval_ownerself = vehicle_change_intval_tem1.groupby('mobile')['buycar_interval'].mean().reset_index(name="buycar_interval_mean")
    # -群的平均間隔
    vehicle_change_intval_tem1 = vehicle_change_intval_tem1.join(vehicle_change_intval_ownerself.loc[:,["mobile", "buycar_interval_mean"]].set_index('mobile'), on='mobile')
    vehicle_change_intval_level_tem =vehicle_change_intval_tem1[['mobile', 'value_class', 'buycar_interval_mean']]
    vehicle_change_intval_level_tem1 = vehicle_change_intval_level_tem[vehicle_change_intval_level_tem["value_class"] != '未知']
    vehicle_change_intval_level = vehicle_change_intval_level_tem1.groupby('value_class')['buycar_interval_mean'].mean().reset_index(name="buycar_interval_levelmean")

    # 決定使用哪個換車間隔
    cmain_lxg_people_intval1 = cmain_lxg_people_change.join(vehicle_change_intval_ownerself.loc[:,["mobile","buycar_interval_mean"]].set_index('mobile'), on='mobile')
    cmain_lxg_people_intval = cmain_lxg_people_intval1.join(vehicle_change_intval_level.loc[:,["value_class","buycar_interval_levelmean"]].set_index('value_class'), on='value_class')
    cmain_lxg_people_intval['buycar_interval_used'] = np.where(cmain_lxg_people_intval['buycar_interval_mean'].isnull(), cmain_lxg_people_intval['buycar_interval_levelmean'], cmain_lxg_people_intval['buycar_interval_mean'])


    # #### 購車熟度

    #將間隔時間單位由天換成年
    cmain_lxg_people_intval['buycar_interval_used_y'] = cmain_lxg_people_intval['buycar_interval_used']/365.25
    cmain_lxg_people_buycar_maturity = vehicle_change_latest.join(cmain_lxg_people_intval.loc[:,["mobile", "buycar_interval_used_y"]].set_index('mobile'), on='mobile')
    cmain_lxg_people_buycar_maturity['carage_last'] = (today - pd.to_datetime(cmain_lxg_people_buycar_maturity['deliverydate']).dt.date).transform(get_caryears)
    cmain_lxg_people_buycar_maturity['buycar_maturity'] = round((cmain_lxg_people_buycar_maturity['carage_last']  / cmain_lxg_people_buycar_maturity['buycar_interval_used_y'])* 100, 1)



    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(cmain_lxg_people_buycar_maturity.loc[:,["mobile", "buycar_maturity"]].set_index('mobile'), on='mobile')

    cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # #### 購車熟度等級

    def buycar_maturity(bcmaturity):
        if (bcmaturity <= 80) & (np.isnan(bcmaturity)):
            return '低熟度'
        elif (bcmaturity > 80) & (bcmaturity <= 120):
            return '中熟度'
        elif (bcmaturity > 120):
            return '高熟度'         
        else:
            return None



    cmain_lxg_people_buycar_maturity['maturity_level'] = cmain_lxg_people_buycar_maturity['buycar_maturity'].transform(buycar_maturity)
    lxg_buycar_maturity = cmain_lxg_people_buycar_maturity[["mobile", "buycar_maturity", "maturity_level"]]

    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(lxg_buycar_maturity.loc[:,["mobile", "maturity_level"]].set_index('mobile'), on='mobile')


    cmain_lxg_people_membertag = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')

    # ### 試駕資料

    # ##### 近1年
    # 計算成為試駕日期和資料更新日期的差幾天(days)
    # lxg_nv_testdrive['day_diff'] =  today - pd.to_datetime(lxg_nv_testdrive['testdrivedate'], unit='D', origin='1970-1-1').dt.date
    lxg_nv_testdrive['day_diff'] =  today - pd.to_datetime(lxg_nv_testdrive['testdrivedate']).dt.date
    lxg_nv_testdrive['day_diff'] =  lxg_nv_testdrive['day_diff'].dt.days

    # 篩1年內的試駕紀錄
    lxg_nv_testdrive_oneyear = lxg_nv_testdrive[(lxg_nv_testdrive['day_diff'] >= 0 ) & (lxg_nv_testdrive['day_diff'] <=360)]
    lxg_nv_testdrive_oneyear['is_testd'] = 1


    #  人表 join 試駕表，未剔除法人和二手車商
    cmain_lxg_people_testdrive_ctb = cmain_lxg_people.merge(lxg_nv_testdrive_oneyear, left_on='mobile', right_on='mobileno_09',how='left')

    # 最近1年試駕次數
    cmain_lxg_people_testdrive_ctb = cmain_lxg_people_testdrive_ctb.groupby('mobile')['is_testd'].sum().reset_index(name="ttestdrive_freq")


    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(cmain_lxg_people_testdrive_ctb.loc[:,["mobile", "ttestdrive_freq"]].set_index('mobile'), on='mobile')

    cmain_lxg_people_membertag = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')

    # ##### 最近1次
    lxg_nv_testdrive_latest = lxg_nv_testdrive_oneyear.copy()
    # lxg_nv_testdrive_latest['tf_testdrivedate'] = pd.to_datetime(lxg_nv_testdrive_latest['testdrivedate'], unit='D', origin='1970-1-1').dt.date
    lxg_nv_testdrive_latest['tf_testdrivedate'] = pd.to_datetime(lxg_nv_testdrive_latest['testdrivedate']).dt.date

    lxg_nv_testdrive_latest['tf_testdrivedate']  = pd.to_datetime(lxg_nv_testdrive_latest['tf_testdrivedate'] )
    lxg_nv_testdrive_latest = lxg_nv_testdrive_latest.loc[lxg_nv_testdrive_latest.groupby('mobileno_09')['tf_testdrivedate'].idxmax()]
    lxg_nv_testdrive_latest_td = lxg_nv_testdrive_latest[["mobileno_09", "testdrivedate", "tf_testdrivedate"]]
    lxg_nv_testdrive_latest_td.rename(columns = {'tf_testdrivedate':'maxtf_testdrivedate', 'testdrivedate':'max_testdrivedate'}, inplace = True)
    lxg_nv_testdrive_latest_td = lxg_nv_testdrive_latest_td[["mobileno_09", "maxtf_testdrivedate", "max_testdrivedate"]].copy()
    lxg_nv_testdrive_latest_td['day_to_lattdrive'] = (today - pd.to_datetime(lxg_nv_testdrive_latest_td['maxtf_testdrivedate']).dt.date)


    # cmain_lxg_people = cmain_lxg_people.join(lxg_nv_testdrive_latest_td.loc[:,["mobileno", "maxtf_testdrivedate", "day_to_lattdrive"]].set_index('mobileno'), on='mobile')

    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(lxg_nv_testdrive_latest_td.loc[:,["mobileno_09", "day_to_lattdrive"]].set_index('mobileno_09'), on='mobile')

    cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # ### 車主距開發權服務廠距離

    lxg_ower_splant_distance_tem1 = lxg_ower_splant_distance.copy()

    lxg_ower_splant_distance_tem1["f_to_oa_distance_km"].dtypes


    lxg_ower_splant_distance_tem1["f_to_oa_distance_km_fl"] = round(lxg_ower_splant_distance_tem1["f_to_oa_distance_km"].astype(float), 1)




    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(lxg_ower_splant_distance_tem1.loc[:,["master_mobile", "f_to_oa_distance_km_fl"]].set_index('master_mobile'), on='mobile')
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.rename(columns={"f_to_oa_distance_km_fl": "dept_distance"})




    cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # ### 車主居住地經緯度



    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(z_lxg_latlon.loc[:,["master_mobile", "longitude"]].set_index('master_mobile'), on='mobile')
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(z_lxg_latlon.loc[:,["master_mobile", "latitude"]].set_index('master_mobile'), on='mobile')




    cmain_lxg_people_membertag=cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # ### 作業系統

    # website_attr = dac_website_log.copy()
    # website_attr_phone = website_attr.merge(hash_map, left_on='sitemember', right_on='hash1',how='left')



    # cmain_lxg_people_device = cmain_lxg_people[["mobile"]]
    # people_website_attr = cmain_lxg_people_device.merge(website_attr_phone, left_on='mobile', right_on='mobile',how='left')



    # dbug_1 = people_website_attr['created_time']


    # people_website_attr['used_device_name']  = people_website_attr['devicetype']
    # # 檢查時區
    # people_website_attr['created_time_tf'] = pd.to_datetime(people_website_attr['created_time'], errors='coerce', utc=True )
    # people_website_attr['used_device_dayname'] = people_website_attr['created_time_tf'].dt.day_name().transform(change_language)
    # people_website_attr['used_device_time'] = people_website_attr['created_time_tf'].dt.strftime("%H:%M")




    # dbug_1 = website_attr.head(10000)
    # dbug_2 = people_website_attr[['mobile','devicetype', 'created_time']]
    # dbug_3 = people_website_attr[['mobile','devicetype', 'created_time', 'created_time_tf', 'used_device_dayname', 'used_device_time']]
    # dbug_1.to_csv('../execution_output/dbug_1.csv', index=False)
    # dbug_2.to_csv('../execution_output/dbug_2.csv', index=False)
    # dbug_3.to_csv('../execution_output/dbug_3.csv', index=False)




    # cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(people_website_attr.loc[:,["mobile", "used_device_name"]].set_index('mobile'), on='mobile')



    # cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    # cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # ##### 最常上網周段前三名


    # people_website_attr['used_device_dayname_yn'] = np.where(people_website_attr['used_device_dayname'].notnull(), 1 , 0)
    # people_website_attr_weekdaysum = people_website_attr.groupby(['mobile', 'used_device_dayname'])['used_device_dayname_yn'].sum().reset_index(name="used_device_dayname_sum")
    # people_website_attr_weekdaysum_f3 = people_website_attr_weekdaysum.sort_values('used_device_dayname_sum', ascending = False).groupby('mobile').head(3)



    # cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(people_website_attr_weekdaysum_f3.loc[:,["mobile", "used_device_dayname"]].set_index('mobile'), on='mobile')




    # cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    # cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # ##### 最常上網時段前三名

    def get_time_interval(ctime):
        if pd.isna(ctime):
            return '未知'
        else:
            if '00:00'<= ctime < '01:00' :
                return '00:00-01:00'
            elif '01:00'<= ctime < '02:00' :
                return '01:00-02:00'
            elif '02:00'<= ctime < '03:00' :
                return '02:00-03:00'
            elif '03:00'<= ctime < '04:00' :
                return '03:00-04:00'
            elif '04:00'<= ctime < '05:00' :
                return '04:00-05:00'
            elif '05:00'<= ctime < '06:00' :
                return '05:00-06:00'
            elif '06:00'<= ctime < '07:00' :
                return '06:00-07:00'
            elif '07:00'<= ctime < '08:00' :
                return '07:00-08:00'
            elif '08:00'<= ctime < '09:00' :
                return '08:00-09:00'
            elif '09:00'<= ctime < '10:00' :
                return '09:00-10:00'
            elif '10:00'<= ctime < '11:00' :
                return '10:00-11:00'
            elif '11:00'<= ctime < '12:00' :
                return '11:00-12:00'
            elif '12:00'<= ctime < '13:00' :
                return '12:00-13:00'
            elif '13:00'<= ctime < '14:00' :
                return '13:00-14:00'
            elif '14:00'<= ctime < '15:00' :
                return '14:00-15:00'
            elif '15:00'<= ctime < '16:00' :
                return '15:00-16:00'
            elif '16:00'<= ctime < '17:00' :
                return '16:00-17:00'
            elif '17:00'<= ctime < '18:00' :
                return '17:00-18:00'
            elif '18:00'<= ctime < '19:00' :
                return '18:00-19:00'
            elif '19:00'<= ctime < '20:00' :
                return '19:00-20:00'
            elif '20:00'<= ctime < '21:00' :
                return '20:00-21:00'
            elif '21:00'<= ctime < '22:00' :
                return '21:00-22:00'
            elif '22:00'<= ctime < '23:00' :
                return '22:00-23:00'
            elif '23:00'<= ctime < '24:00' :
                return '23:00-24:00'
            




    # people_website_attr['used_device_time_interval'] = people_website_attr['used_device_time'].transform(get_time_interval)



    # people_website_attr['time_interval_yn'] = np.where(people_website_attr['used_device_dayname'].notnull(), 1 , 0)
    # people_website_attr_timesum = people_website_attr.groupby(['mobile', 'used_device_time_interval'])['time_interval_yn'].sum().reset_index(name="used_device_time_interval_sum")
    # people_website_attr_timesum_f3 = people_website_attr_timesum.sort_values('used_device_time_interval_sum', ascending = False).groupby('mobile').head(3)



    # cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(people_website_attr_timesum_f3.loc[:,["mobile", "used_device_time_interval"]].set_index('mobile'), on='mobile')


    # cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    # cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # #### n7預購名單和退訂資料



    # 原始訂單資料 lxg_n7_order_1st / lxg_n7_order_2nd
    lxg_n7_order_2nd = lxg_n7_order_2nd.drop(columns='授權交易單號')
    # 退訂資料 lxg_n7_order_cancel_new
    # 盲盒領取和開啟資料 lxg_n7_nft_list 

    # 最新n7相關活動紀錄
    activity_7m = lxg_n7act_weight


    # #### 原訂單總數


    # 原始訂單資料
    lxg_n7_order_ori = lxg_n7_order_1st.append(lxg_n7_order_2nd, ignore_index=True)



    # 先計算每個預購者、每張專案編號之訂單數
    lxg_n7_order_ori_eft = lxg_n7_order_ori[lxg_n7_order_ori['綠界付款狀態'] == '專案成立']
    lxg_n7_order_ori_eft_ordernum= lxg_n7_order_ori_eft.groupby(['手機號碼', '專案編號'])['總金額'].sum().reset_index(name="cost_sum")
    lxg_n7_order_ori_eft_ordernum['order_num'] = lxg_n7_order_ori_eft_ordernum['cost_sum'] / 1000
    lxg_n7_order_ori_eft_ordernum['order_num'] = lxg_n7_order_ori_eft_ordernum['order_num'].astype('int')




    n7_totalorders = lxg_n7_order_ori_eft_ordernum.copy()
    # 原總預購張數 25,500張
    n7_order_totalnum= n7_totalorders['order_num'].sum()
    # 原預購人數 23,200人
    n7_order_people = n7_totalorders.drop_duplicates('手機號碼', keep='first')
    n7_order_people_num =n7_order_people['手機號碼'].count()




    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(n7_order_people.loc[:,["手機號碼", "order_num"]].set_index('手機號碼'), on='de_columnname')
    cmain_lxg_people_membertag["n7_preorder_start"] = cmain_lxg_people_membertag["order_num"].transform(lambda x: '是' if x >= 1 else '否')



    cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # #### 退訂訂單


    lxg_n7_order_cancel_new['cancel_num'] = (lxg_n7_order_cancel_new['總金額'] / 1000).astype('int')



    # group by 手機號碼和專案編號，計算退訂數
    lxg_n7_order_cancel_new_calcelnum= lxg_n7_order_cancel_new.groupby(['手機號碼', '專案編號'])['cancel_num'].sum().reset_index(name="calcel_sum")


    # #### 計算殘留人數


    n7_totalorders_rt = n7_totalorders.merge(lxg_n7_order_cancel_new_calcelnum, left_on=['手機號碼', '專案編號'], right_on=['手機號碼', '專案編號'],how='left')


    n7_totalorders_rt['diff_num']  = n7_totalorders_rt['order_num'].sub(n7_totalorders_rt['calcel_sum'], fill_value=0).fillna(0)



    n7_totalorders_rt_retent_tem = n7_totalorders_rt[n7_totalorders_rt['diff_num'] > 0 ] 
    n7_totalorders_rt_retent_tem1 = n7_totalorders_rt_retent_tem.drop_duplicates('手機號碼', keep='first')
    n7_retention = n7_totalorders_rt_retent_tem1

    # retention人數
    n7_totalorders_new_retent_people = n7_totalorders_rt_retent_tem1['手機號碼'].count()



    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(n7_retention.loc[:,["手機號碼", "diff_num"]].set_index('手機號碼'), on='de_columnname')
    cmain_lxg_people_membertag["n7_preorder_now"] = cmain_lxg_people_membertag["diff_num"].transform(lambda x: '是' if x >= 1 else '否')



    def n7orderyn(status1, status2 ):
        if (pd.isnull(status1)) & (pd.isnull(status2)) :
            return None
        elif (pd.notnull(status1)) & (pd.notnull(status2)) & ((status1 =='是') & (status2 =='否')):
            return '曾預購'
        elif (pd.notnull(status1)) & (pd.notnull(status2)) & ((status1 =='是') & (status2 =='是')):
            return '是'
        elif (pd.notnull(status1)) & (pd.notnull(status2)) & ((status1 =='否') & (status2 =='否')):
            return '否'        



    cmain_lxg_people_membertag['n7_preorder'] = cmain_lxg_people_membertag.apply(lambda x: n7orderyn(x['n7_preorder_start'], x['n7_preorder_now']), axis=1)



    cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')


    # #### Line UID 、生日和城市資料


    lxg_web_member.rename(columns={'de_columnname':'phone'} , inplace=True)



    n7_baisc_info = lxg_web_member[['phone','line_uid', 'birthday', 'city']]



    # merge 預購資料和基本資訊
    n7_retention_binfo = n7_retention.merge(n7_baisc_info, left_on='手機號碼', right_on='phone',how='left')


    # #### 依基本資訊給分


    def n7_engagement(phone, lineuid, bday, city):
        if (pd.isnull(lineuid)) & (pd.isnull(bday)) & (pd.isnull(city)):
            return 'L'
        elif (pd.notnull(lineuid)) & (pd.notnull(bday)) & (pd.notnull(city)):
            return 'H'
        elif ((pd.notnull(lineuid)) | (pd.notnull(bday)) | (pd.notnull(city))):
            return 'M'

    n7_retention_binfo['engagement'] = n7_retention_binfo.apply(lambda x: n7_engagement(x['phone'], x['line_uid'], x['birthday'], x['city']), axis=1)   



    n7_retention_binfo_dis = n7_retention_binfo.drop_duplicates('手機號碼', keep='first')
    # n7_retention_binfo_dis['engagement'].value_counts()


    # #### 活動分數

    # ##### 盲盒領取


    web_member_uuid = lxg_web_member[["uuid", 'phone']]
    # 1隻手機有兩個uuid

    nft_list = lxg_n7_nft_list.merge(web_member_uuid, left_on='uuid', right_on='uuid',how='left')

    nft_list1 = nft_list[["phone", "mint_date"]]
    nft_list1 = nft_list1.rename(columns={"mint_date": "date"})
    nft_list1['jenny_weight'] = 3
    nft_list2 = nft_list[["phone", "unblind_date"]]
    nft_list2 = nft_list2.rename(columns={"unblind_date": "date"})
    nft_list2['date_ymd'] = pd.to_datetime(nft_list2["date"]).dt.date
    start_date = date(2023, 1, 1)
    nft_list2 = nft_list2[nft_list2["date_ymd"] != start_date]
    nft_list2['jenny_weight'] = 2


    nft_act = pd.concat([nft_list1,nft_list2], ignore_index=True)


    # ##### lxg_web_linemembertag


    # lxg_web_linemembertag_n7 = lxg_web_linemembertag
    # lxg_web_linemembertag_n7_y = lxg_web_linemembertag_n7.loc[lxg_web_linemembertag_n7['tag_name'].isin(activity_7m['n7_activities'])]

    # lxg_web_linemembertag_n7_y_dis=lxg_web_linemembertag_n7_y.drop_duplicates(['line_uid', 'tag_id'], keep='first')

    # lxg_web_linemembertag_n7_basic_info = n7_retention_binfo_dis.merge(lxg_web_linemembertag_n7_y_dis, left_on='line_uid', right_on='line_uid',how='left')


    # n7_linemembertag_activity = lxg_web_linemembertag_n7_basic_info.merge(activity_7m, left_on='tag_name', right_on='n7_activities',how='left')



    def n7_act_score(daydiff, jwt):
        if  0 <= daydiff <= 90:
            return jwt * 1.5
        elif 91 <= daydiff <= 180:
            return jwt * 1
        elif 181 <= daydiff:
            return jwt * 0.5




    # # 計算訂單活動日期和資料更新日期差幾天(days)
    # n7_linemembertag_activity['day_diff'] =  (today - pd.to_datetime(n7_linemembertag_activity['date']).dt.date)
    # n7_linemembertag_activity['day_diff'] =  (today - pd.to_datetime(n7_linemembertag_activity['date']).dt.date)

    # n7_linemembertag_activity['day_diff'] =  n7_linemembertag_activity['day_diff'].dt.days  
    # # 計算活動分數
    # n7_linemembertag_activity['act_score'] = n7_linemembertag_activity.apply(lambda x: n7_act_score(x['day_diff'], x['jenny_weight']), axis=1)


    # n7_linemembertag_activity_dis = n7_linemembertag_activity.drop_duplicates(['手機號碼', 'tag_id'], keep='first')


    # 計算盲盒活動日期和資料更新日期差幾天(days)
    nft_act['day_diff'] =  (today - pd.to_datetime(nft_act['date']).dt.date)
    nft_act['day_diff'] =  nft_act['day_diff'].dt.days  
    # 計算活動分數
    nft_act['act_score'] = nft_act.apply(lambda x: n7_act_score(x['day_diff'], x['jenny_weight']), axis=1)
    nft_act = nft_act[nft_act['phone'].notnull()]


    n7_retention_binfo_dis_phone = n7_retention_binfo_dis[["手機號碼"]]
    nft_act_retention = n7_retention_binfo_dis_phone.merge(nft_act, left_on='手機號碼', right_on='phone',how='left')



    # n7_linemembertag_act = n7_linemembertag_activity_dis[['手機號碼', 'act_score']]
    nft_act_retention_y = nft_act_retention[['手機號碼', 'act_score']]

    # n7_activity_score= pd.concat([n7_linemembertag_act, nft_act_retention_y],  ignore_index=True)
    n7_activity_score=  nft_act_retention_y.copy()


    # n7_activity_score = n7_activity_score_tem.groupby('手機號碼')['act_score'].sum().reset_index(name="total_score")
    n7_activity_score["total_score"] = n7_activity_score.groupby('手機號碼')['act_score'].transform('sum', min_count=1)
    n7_activity_score_dis = n7_activity_score.drop_duplicates(['手機號碼'], keep='first')


    def n7_hotlevel(score, engage):
        if (10 < score) & (engage == 'H'):
            return 'EHVH'
        elif (5 < score <= 10 ) & (engage == 'H'):
            return 'EHVM'
        elif (0 <= score <= 5 ) & (engage == 'H'):
            return 'EHVL'
        elif np.isnan(score) & (engage == 'H'):
            return 'EHVN'
        elif (10 < score) & (engage == 'M'):
            return 'EMVH'
        elif (5 < score <= 10 ) & (engage == 'M'):
            return 'EMVM'
        elif (0 <= score <= 5 ) & (engage == 'M'):
            return 'EMVL'
        elif np.isnan(score) & (engage == 'M'):
            return 'EMVN'
        elif (10 < score) & (engage == 'L'):
            return 'ELVH'
        elif (5 < score <= 10 ) & (engage == 'L'):
            return 'ELVM'
        elif (0 <= score <= 5 ) & (engage == 'L'):
            return 'ELVL'
        elif np.isnan(score) & (engage == 'L'):
            return 'ELVN'     


    def n7_hotlevel_tem(score, engage):
        if (10 < score) & (engage == 'H'):
            return '1'
        elif (5 < score <= 10 ) & (engage == 'H'):
            return '3'
        elif (0 <= score <= 5 ) & (engage == 'H'):
            return '7'
        elif np.isnan(score) & (engage == 'H'):
            return '10'
        elif (10 < score) & (engage == 'M'):
            return '2'
        elif (5 < score <= 10 ) & (engage == 'M'):
            return '4'
        elif (0 <= score <= 5 ) & (engage == 'M'):
            return '8'
        elif np.isnan(score) & (engage == 'M'):
            return '11'
        elif (10 < score) & (engage == 'L'):
            return 'ELVH'
        elif (5 < score <= 10 ) & (engage == 'L'):
            return 'ELVM'
        elif (0 <= score <= 5 ) & (engage == 'L'):
            return '9'
        elif np.isnan(score) & (engage == 'L'):
            return '12'


    # join engagement 和 活動分數
    n7_value = n7_retention_binfo_dis.merge(n7_activity_score_dis, left_on='手機號碼', right_on='手機號碼', how='left')
    n7_value['n7_preoder_level'] = n7_value.apply(lambda x: n7_hotlevel(x['total_score'], x['engagement']), axis=1)

    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(n7_value.loc[:,["手機號碼", "n7_preoder_level"]].set_index('手機號碼'), on='mobile')
    # n7_value['n7_preoder_level'].value_counts()

    # ### 納智捷車主且曾去街邊保修

    cmain_lxg_people_mlf = cmain_lxg_people.copy()
    cmain_lxg_people_mlf['is_lxg_mlf_member'] =np.where(cmain_lxg_people_mlf['mobile'].isin(cmain_mlf_people['phone']), '是', '否')
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.join(cmain_lxg_people_mlf.loc[:,["mobile", "is_lxg_mlf_member"]].set_index('mobile'), on='mobile')


    cmain_lxg_people_membertag  = cmain_lxg_people_membertag[cmain_lxg_people_membertag.iloc[:, 2:].notnull().any(axis=1)]
    cmain_lxg_people_membertag = cmain_lxg_people_membertag.drop_duplicates(keep='first')



    # lxg_mlf = cmain_lxg_people_mlf[cmain_lxg_people_mlf['is_lxg_mlf_member'] == '是']
    # lxg_mlf = lxg_mlf[['yl_id']]
    # lxg_mlf


    # #### 格上有電動車偏好


    cmain_cpt_people_ev = cmain_cpt_people.copy()


    cmain_cpt_people_ev = cmain_cpt_people_ev.join(n7_order_people.loc[:,["手機號碼", "order_num"]].set_index('手機號碼'), on='de_columnname')
    cmain_cpt_people_ev["cpt_preorder_start"] = cmain_cpt_people_ev["order_num"].transform(lambda x: '是' if x >= 1 else '否')

    cmain_cpt_people_ev = cmain_cpt_people_ev.join(n7_retention.loc[:,["手機號碼", "diff_num"]].set_index('手機號碼'), on='de_columnname')
    cmain_cpt_people_ev["cpt_preorder_now"] = cmain_cpt_people_ev["diff_num"].transform(lambda x: '是' if x >= 1 else '否')




    cmain_cpt_people_ev["cpt_preorder_start"].value_counts()



    cmain_cpt_people_ev['n7_preorder'] = cmain_cpt_people_ev.apply(lambda x: n7orderyn(x['cpt_preorder_start'], x['cpt_preorder_now']), axis=1)




    cmain_cpt_people_ev['n7_preorder'].value_counts()


    # # tag_table 1


    tag_names_mapping = {
        'yl_id' : 'yl_id',
        'sso_id': 'ID資訊_Single sign-on ID',
        'is_lxg_member': '會員資訊_是否為納智捷會員',
        'opp_status': '會員資訊_納智捷潛客狀態',
        'is_lxg_car_owner': '會員資訊_是否為納智捷車主',
        'is_lxg_car_driver': '會員資訊_是否為納智捷駕駛',
        'is_line_id': '社群資訊_是否為LINE會員',
        'registration_channel': '註冊資訊_註冊來源',
        'registration_time': '註冊資訊_首次註冊日期',
        # 'is_email': '聯繫方式_是否有電子郵件',
        'age' : '年齡_年齡',
        'age_range': '年齡_年齡區間(5歲)',
        'gender': '性別_性別',
        'residence_city': '居住資訊_縣市',
        'residence_district': '居住資訊_鄉鎮',
        'longitude':'居住資訊_居住地經度',
        'latitude':'居住資訊_居住地緯度',
        # 'incomelevel':'收入_月收入區間',
        # 'marriagestatus':'婚姻資訊_婚姻狀態',
        # 'used_device_name' : '上網使用習慣_作業系統', # no data after Jan, 2024
        # 'used_device_dayname' : '上網使用習慣_經常上網週段(Top3)', # no data after Jan, 2024
        # 'used_device_time_interval' :'上網使用習慣_經常上網時段(Top3)', # no data after Jan, 2024
        'contribution': '會員分級_會員貢獻度',
        'loyalty': '會員分級_會員忠誠度',
        'value_class':'車主分級_車主價值',
        'buycar_maturity':'車主分級_車主購車熟度',
        'maturity_level':'車主分級_車主購車熟度等級',
        'n7_preoder_level':'會員分級_n7預購溫度',
        'n7_preorder':'預購資訊_是否預購n7',
        'is_owncar': '人車資訊_名下是否有車',
        'is_warrantyexpiry': '人車資訊_是否為過保固車',
        'is_insuranceexpiry': '人車資訊_是否為過保險車',
        'odoreading_mean_final':'人車資訊_車子使用程度',
        'is_bonus' : '會員點數_是否尚有紅利點數',
        'lxg_first_opp_time': '有望客資訊_首次成為潛客時間',
        'lxg_last_opp_time': '有望客資訊_最近成為潛客時間',
        'lxg_first_test_drive_time': '有望客資訊_首次試駕日期',
        'lxg_last_test_drive_time': '有望客資訊_首次試駕日期',
        'day_to_lattdrive': '有望客資訊_距最近一次試駕天數',
        'ttestdrive_freq':'有望客資訊_近一年試駕次數',
        'lxg_first_buy_time': '購車資訊_首次買車日期',
        'lxg_last_buy_time': '購車資訊_最近買車日期',
        'lxg_first_deliver_time': '購車資訊_首次交車日期',
        'lxg_last_deliver_time': '購車資訊_最近交車日期',
        'dept_distance': '售服資訊_車主距開發權服務廠距離',
        'returned_days': '售服資訊_距上次進廠天數',
        'is_lxg_mlf_member': '會員資訊_去過街邊保修'
        }

    tmp_table1 = cmain_lxg_people_membertag.loc[:, list(tag_names_mapping.keys())].copy()
    tmp_table1.rename(columns = tag_names_mapping, inplace = True)


    # # Reshape 1

    tag_table1 = tmp_table1.set_index('yl_id').stack().reset_index(name='value')
    tag_table1.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
    tag_table1['bcat'] = '會員屬性'
    tag_table1['mcat'] = tag_table1['tag_name'].transform(lambda x: x.split('_')[0])
    tag_table1['scat'] = tag_table1['tag_name'].transform(lambda x: x.split('_')[1])
    tag_table1 = tag_table1.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
    tag_table1['crt_dt'] = datetime.now()
    tag_table1['upd_dt'] = datetime.now()


    tag_table1=tag_table1[tag_table1.iloc[:, 2:].notnull().any(axis=1)]
    tag_table1 = tag_table1.drop_duplicates(keep='first')


    # # tag_table 2

    tag_names_mapping2 = {
        'yl_id' : 'yl_id',
        'rep_buy': '再購資訊_是否重複購車',
        'car_count': '人車資訊_擁車數量',
        'carage': '人車資訊_車齡',
        'is_bonusexpiry': '會員點數_紅利點數是否到期'
        }

    tmp_table2 = cmain_lxg_people_membertag2.loc[:, list(tag_names_mapping2.keys())].copy()
    tmp_table2.rename(columns = tag_names_mapping2, inplace = True)


    # # reshape 2

    tag_table2 = tmp_table2.set_index('yl_id').stack().reset_index(name='value')
    tag_table2.rename(columns = { 'level_1': 'tag_name', 'value': 'tag_value'}, inplace = True)
    tag_table2['bcat'] = '會員屬性'
    tag_table2['mcat'] = tag_table2['tag_name'].transform(lambda x: x.split('_')[0])
    tag_table2['scat'] = tag_table2['tag_name'].transform(lambda x: x.split('_')[1])
    tag_table2 = tag_table2.loc[:,['yl_id', 'bcat', 'mcat', 'scat', 'tag_name', 'tag_value']]
    tag_table2['crt_dt'] = datetime.now()
    tag_table2['upd_dt'] = datetime.now()


    tag_table2=tag_table2[tag_table2.iloc[:, 2:].notnull().any(axis=1)]
    tag_table2 = tag_table2.drop_duplicates(keep='first')


    tag_table = pd.concat([tag_table1,tag_table2], ignore_index=True)

    tag_table = tag_table[tag_table["tag_value"] != '未知']
    tag_table = tag_table[tag_table["tag_value"].notnull()]
    tag_table = tag_table[tag_table["tag_value"] != '']


    # ### 串tag_id


    brand_tag_define_lxg = brand_tag_define[brand_tag_define['brand_name'] == '納智捷']

    tag_table = tag_table.join(brand_tag_define_lxg.loc[:,["tag_name", "tag_id"]].set_index('tag_name'), on='tag_name')


    # ### 串 luxgen 官網會員id

    # cmain_lxg_people_membertag_tem = cmain_lxg_people_membertag[["mobile", "yl_id"]]
    # lxg_id = cmain_lxg_people_membertag_tem.join(lxg_web_member.loc[:,["pk", "phone"]].set_index('phone'), on='mobile')
    # lxg_id = lxg_id.rename(columns={"pk": "lxg_id"})


    cmain_lxg_people_lxgid = cmain_lxg_people_lxgid[["yl_id", "memid"]]
    tag_table = tag_table.join(cmain_lxg_people_lxgid.loc[:,["memid", "yl_id"]].set_index('yl_id'), on='yl_id')

    tag_table = tag_table.drop_duplicates(keep='first')
    tag_table.rename(columns={'memid':'lxg_id'}, inplace=True )


    len(tag_table)

    for i in range(0,len(tag_table),10000):
        write_into_tag_table(df = tag_table.iloc[i:i+10000], table_name = 'lxg_tag', schema_name = 'dt_tag', method = 'keep')
        print('The batches are:', '[',  i,  + i+10000, ']', '\n')



default_args = {
    'start_date': datetime(2024, 5, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('lxg_tag_flow_01', schedule_interval='@daily', default_args=default_args) as dag:
    task1 = PythonOperator(
        task_id='lxg_membership',
        python_callable=lxg_membership_task,
        provide_context=True
    )

    task1