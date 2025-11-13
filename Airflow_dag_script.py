import os
import logging
import requests
import pandas as pd
import numpy as np
import shutil
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data"
DIMENSIONS = AIRFLOW_DATA + "/dimensions/"
FACTS = AIRFLOW_DATA + "/facts/"


########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'BDE_ASSIGNMENT',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='bde_assignment3',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################


def import_load_NSW_LGA_CODE_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = DIMENSIONS + 'NSW_LGA_CODE.csv'
    if not os.path.exists(category_file_path):
        logging.info("No NSW_LGA_CODE.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)

    if len(df) > 0:
        col_names = ['LGA_CODE', 'LGA_NAME']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_NSW_LGA_CODE(LGA_CODE, LGA_NAME)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_NSW_LGA_SUBURB_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = DIMENSIONS + 'NSW_LGA_SUBURB.csv'
    if not os.path.exists(category_file_path):
        logging.info("No NSW_LGA_SUBURB.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)

    if len(df) > 0:
        col_names = ['LGA_NAME', 'SUBURB_NAME']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_NSW_LGA_SUBURB(LGA_NAME,SUBURB_NAME)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None   

def import_load_2016Census_G01_NSW_LGA_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = DIMENSIONS + '2016Census_G01_NSW_LGA.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 2016Census_G01_NSW_LGA.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)

    if len(df) > 0:
        col_names = ['LGA_CODE_2016', 'Tot_P_M', 'Tot_P_F', 'Tot_P_P', 'Age_0_4_yr_M',
        'Age_0_4_yr_F', 'Age_0_4_yr_P', 'Age_5_14_yr_M', 'Age_5_14_yr_F',
        'Age_5_14_yr_P', 'Age_15_19_yr_M', 'Age_15_19_yr_F',
        'Age_15_19_yr_P', 'Age_20_24_yr_M', 'Age_20_24_yr_F',
        'Age_20_24_yr_P', 'Age_25_34_yr_M', 'Age_25_34_yr_F',
        'Age_25_34_yr_P', 'Age_35_44_yr_M', 'Age_35_44_yr_F',
        'Age_35_44_yr_P', 'Age_45_54_yr_M', 'Age_45_54_yr_F',
        'Age_45_54_yr_P', 'Age_55_64_yr_M', 'Age_55_64_yr_F',
        'Age_55_64_yr_P', 'Age_65_74_yr_M', 'Age_65_74_yr_F',
        'Age_65_74_yr_P', 'Age_75_84_yr_M', 'Age_75_84_yr_F',
        'Age_75_84_yr_P', 'Age_85ov_M', 'Age_85ov_F', 'Age_85ov_P',
        'Counted_Census_Night_home_M', 'Counted_Census_Night_home_F',
        'Counted_Census_Night_home_P', 'Count_Census_Nt_Ewhere_Aust_M',
        'Count_Census_Nt_Ewhere_Aust_F', 'Count_Census_Nt_Ewhere_Aust_P',
        'Indigenous_psns_Aboriginal_M', 'Indigenous_psns_Aboriginal_F',
        'Indigenous_psns_Aboriginal_P', 'Indig_psns_Torres_Strait_Is_M',
        'Indig_psns_Torres_Strait_Is_F', 'Indig_psns_Torres_Strait_Is_P',
        'Indig_Bth_Abor_Torres_St_Is_M', 'Indig_Bth_Abor_Torres_St_Is_F',
        'Indig_Bth_Abor_Torres_St_Is_P', 'Indigenous_P_Tot_M',
        'Indigenous_P_Tot_F', 'Indigenous_P_Tot_P',
        'Birthplace_Australia_M', 'Birthplace_Australia_F',
        'Birthplace_Australia_P', 'Birthplace_Elsewhere_M',
        'Birthplace_Elsewhere_F', 'Birthplace_Elsewhere_P',
        'Lang_spoken_home_Eng_only_M', 'Lang_spoken_home_Eng_only_F',
        'Lang_spoken_home_Eng_only_P', 'Lang_spoken_home_Oth_Lang_M',
        'Lang_spoken_home_Oth_Lang_F', 'Lang_spoken_home_Oth_Lang_P',
        'Australian_citizen_M', 'Australian_citizen_F',
        'Australian_citizen_P', 'Age_psns_att_educ_inst_0_4_M',
        'Age_psns_att_educ_inst_0_4_F', 'Age_psns_att_educ_inst_0_4_P',
        'Age_psns_att_educ_inst_5_14_M', 'Age_psns_att_educ_inst_5_14_F',
        'Age_psns_att_educ_inst_5_14_P', 'Age_psns_att_edu_inst_15_19_M',
        'Age_psns_att_edu_inst_15_19_F', 'Age_psns_att_edu_inst_15_19_P',
        'Age_psns_att_edu_inst_20_24_M', 'Age_psns_att_edu_inst_20_24_F',
        'Age_psns_att_edu_inst_20_24_P', 'Age_psns_att_edu_inst_25_ov_M',
        'Age_psns_att_edu_inst_25_ov_F', 'Age_psns_att_edu_inst_25_ov_P',
        'High_yr_schl_comp_Yr_12_eq_M', 'High_yr_schl_comp_Yr_12_eq_F',
        'High_yr_schl_comp_Yr_12_eq_P', 'High_yr_schl_comp_Yr_11_eq_M',
        'High_yr_schl_comp_Yr_11_eq_F', 'High_yr_schl_comp_Yr_11_eq_P',
        'High_yr_schl_comp_Yr_10_eq_M', 'High_yr_schl_comp_Yr_10_eq_F',
        'High_yr_schl_comp_Yr_10_eq_P', 'High_yr_schl_comp_Yr_9_eq_M',
        'High_yr_schl_comp_Yr_9_eq_F', 'High_yr_schl_comp_Yr_9_eq_P',
        'High_yr_schl_comp_Yr_8_belw_M', 'High_yr_schl_comp_Yr_8_belw_F',
        'High_yr_schl_comp_Yr_8_belw_P', 'High_yr_schl_comp_D_n_g_sch_M',
        'High_yr_schl_comp_D_n_g_sch_F', 'High_yr_schl_comp_D_n_g_sch_P',
        'Count_psns_occ_priv_dwgs_M', 'Count_psns_occ_priv_dwgs_F',
        'Count_psns_occ_priv_dwgs_P', 'Count_Persons_other_dwgs_M',
        'Count_Persons_other_dwgs_F', 'Count_Persons_other_dwgs_P']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_2016Census_G01_NSW_LGA(LGA_CODE_2016, Tot_P_M, Tot_P_F, Tot_P_P, Age_0_4_yr_M,
                    Age_0_4_yr_F, Age_0_4_yr_P, Age_5_14_yr_M, Age_5_14_yr_F,
                    Age_5_14_yr_P, Age_15_19_yr_M, Age_15_19_yr_F,
                    Age_15_19_yr_P, Age_20_24_yr_M, Age_20_24_yr_F,
                    Age_20_24_yr_P, Age_25_34_yr_M, Age_25_34_yr_F,
                    Age_25_34_yr_P, Age_35_44_yr_M, Age_35_44_yr_F,
                    Age_35_44_yr_P, Age_45_54_yr_M, Age_45_54_yr_F,
                    Age_45_54_yr_P, Age_55_64_yr_M, Age_55_64_yr_F,
                    Age_55_64_yr_P, Age_65_74_yr_M, Age_65_74_yr_F,
                    Age_65_74_yr_P, Age_75_84_yr_M, Age_75_84_yr_F,
                    Age_75_84_yr_P, Age_85ov_M, Age_85ov_F, Age_85ov_P,
                    Counted_Census_Night_home_M, Counted_Census_Night_home_F,
                    Counted_Census_Night_home_P, Count_Census_Nt_Ewhere_Aust_M,
                    Count_Census_Nt_Ewhere_Aust_F, Count_Census_Nt_Ewhere_Aust_P,
                    Indigenous_psns_Aboriginal_M, Indigenous_psns_Aboriginal_F,
                    Indigenous_psns_Aboriginal_P, Indig_psns_Torres_Strait_Is_M,
                    Indig_psns_Torres_Strait_Is_F, Indig_psns_Torres_Strait_Is_P,
                    Indig_Bth_Abor_Torres_St_Is_M, Indig_Bth_Abor_Torres_St_Is_F,
                    Indig_Bth_Abor_Torres_St_Is_P, Indigenous_P_Tot_M,
                    Indigenous_P_Tot_F, Indigenous_P_Tot_P,
                    Birthplace_Australia_M, Birthplace_Australia_F,
                    Birthplace_Australia_P, Birthplace_Elsewhere_M,
                    Birthplace_Elsewhere_F, Birthplace_Elsewhere_P,
                    Lang_spoken_home_Eng_only_M, Lang_spoken_home_Eng_only_F,
                    Lang_spoken_home_Eng_only_P, Lang_spoken_home_Oth_Lang_M,
                    Lang_spoken_home_Oth_Lang_F, Lang_spoken_home_Oth_Lang_P,
                    Australian_citizen_M, Australian_citizen_F,
                    Australian_citizen_P, Age_psns_att_educ_inst_0_4_M,
                    Age_psns_att_educ_inst_0_4_F, Age_psns_att_educ_inst_0_4_P,
                    Age_psns_att_educ_inst_5_14_M, Age_psns_att_educ_inst_5_14_F,
                    Age_psns_att_educ_inst_5_14_P, Age_psns_att_edu_inst_15_19_M,
                    Age_psns_att_edu_inst_15_19_F, Age_psns_att_edu_inst_15_19_P,
                    Age_psns_att_edu_inst_20_24_M, Age_psns_att_edu_inst_20_24_F,
                    Age_psns_att_edu_inst_20_24_P, Age_psns_att_edu_inst_25_ov_M,
                    Age_psns_att_edu_inst_25_ov_F, Age_psns_att_edu_inst_25_ov_P,
                    High_yr_schl_comp_Yr_12_eq_M, High_yr_schl_comp_Yr_12_eq_F,
                    High_yr_schl_comp_Yr_12_eq_P, High_yr_schl_comp_Yr_11_eq_M,
                    High_yr_schl_comp_Yr_11_eq_F, High_yr_schl_comp_Yr_11_eq_P,
                    High_yr_schl_comp_Yr_10_eq_M, High_yr_schl_comp_Yr_10_eq_F,
                    High_yr_schl_comp_Yr_10_eq_P, High_yr_schl_comp_Yr_9_eq_M,
                    High_yr_schl_comp_Yr_9_eq_F, High_yr_schl_comp_Yr_9_eq_P,
                    High_yr_schl_comp_Yr_8_belw_M, High_yr_schl_comp_Yr_8_belw_F,
                    High_yr_schl_comp_Yr_8_belw_P, High_yr_schl_comp_D_n_g_sch_M,
                    High_yr_schl_comp_D_n_g_sch_F, High_yr_schl_comp_D_n_g_sch_P,
                    Count_psns_occ_priv_dwgs_M, Count_psns_occ_priv_dwgs_F,
                    Count_psns_occ_priv_dwgs_P, Count_Persons_other_dwgs_M,
                    Count_Persons_other_dwgs_F, Count_Persons_other_dwgs_P)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None  

def import_load_2016Census_G02_NSW_LGA_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = DIMENSIONS + '2016Census_G02_NSW_LGA.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 2016Census_G02_NSW_LGA.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)

    if len(df) > 0:
        col_names = ['LGA_CODE_2016', 'Median_age_persons',
       'Median_mortgage_repay_monthly', 'Median_tot_prsnl_inc_weekly',
       'Median_rent_weekly', 'Median_tot_fam_inc_weekly',
       'Average_num_psns_per_bedroom', 'Median_tot_hhd_inc_weekly',
       'Average_household_size']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_2016Census_G02_NSW_LGA(LGA_CODE_2016, Median_age_persons,
                    Median_mortgage_repay_monthly,Median_tot_prsnl_inc_weekly,Median_rent_weekly, 
                    Median_tot_fam_inc_weekly,Average_num_psns_per_bedroom, Median_tot_hhd_inc_weekly,
                    Average_household_size)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None 

def import_load_LISTINGS_05_2020_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '05_2020.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 05_2020.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==5) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2020)]
    ## some csv contains data not belonging to the time period; these are ignored to preserve data integrity
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_06_2020_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '06_2020.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 06_2020.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==6) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2020)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_07_2020_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '07_2020.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 07_2020.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==7) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2020)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_08_2020_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '08_2020.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 08_2020.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==8) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2020)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_09_2020_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '09_2020.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 09_2020.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==9) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2020)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_10_2020_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '10_2020.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 10_2020.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==10) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2020)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_11_2020_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '11_2020.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 11_2020.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==11) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2020)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_12_2020_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '12_2020.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 12_2020.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==12) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2020)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_01_2021_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '01_2021.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 01_2021.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==1) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2021)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_02_2021_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '02_2021.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 02_2021.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==2) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2021)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_03_2021_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '03_2021.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 03_2021.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==3) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2021)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None

def import_load_LISTINGS_04_2021_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = FACTS + '04_2021.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 04_2021.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df=df[(pd.to_datetime(df.SCRAPED_DATE).dt.month==4) & (pd.to_datetime(df.SCRAPED_DATE).dt.year==2021)]
    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
       'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
       'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE',
       'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
       'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING',
       'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
       'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
       'REVIEW_SCORES_VALUE']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO RAW.RAW_LISTINGS(LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                    HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30,NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING,
                    REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION,
                    REVIEW_SCORES_VALUE)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None
#########################################################
#
#   DAG Operator Setup
#
#########################################################


import_load_NSW_LGA_CODE_task = PythonOperator(
    task_id="import_load_NSW_LGA_CODE",
    python_callable=import_load_NSW_LGA_CODE_func,
    provide_context=True,
    dag=dag
)


import_load_NSW_LGA_SUBURB_task= PythonOperator(
    task_id="import_load_NSW_LGA_SUBURB",
    python_callable=import_load_NSW_LGA_SUBURB_func,
    provide_context=True,
    dag=dag
)

import_load_2016Census_G01_NSW_LGA_task= PythonOperator(
    task_id="import_load_2016Census_G01_NSW_LGA",
    python_callable=import_load_2016Census_G01_NSW_LGA_func,
    provide_context=True,
    dag=dag
)

import_load_2016Census_G02_NSW_LGA_task= PythonOperator(
    task_id="import_load_2016Census_G02_NSW_LGA",
    python_callable=import_load_2016Census_G02_NSW_LGA_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_05_2020_task= PythonOperator(
    task_id="import_load_LISTINGS_05_2020",
    python_callable=import_load_LISTINGS_05_2020_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_06_2020_task= PythonOperator(
    task_id="import_load_LISTINGS_06_2020",
    python_callable=import_load_LISTINGS_06_2020_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_07_2020_task= PythonOperator(
    task_id="import_load_LISTINGS_07_2020",
    python_callable=import_load_LISTINGS_07_2020_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_08_2020_task= PythonOperator(
    task_id="import_load_LISTINGS_08_2020",
    python_callable=import_load_LISTINGS_08_2020_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_09_2020_task= PythonOperator(
    task_id="import_load_LISTINGS_09_2020",
    python_callable=import_load_LISTINGS_09_2020_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_10_2020_task= PythonOperator(
    task_id="import_load_LISTINGS_10_2020",
    python_callable=import_load_LISTINGS_10_2020_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_11_2020_task= PythonOperator(
    task_id="import_load_LISTINGS_11_2020",
    python_callable=import_load_LISTINGS_11_2020_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_12_2020_task= PythonOperator(
    task_id="import_load_LISTINGS_12_2020",
    python_callable=import_load_LISTINGS_12_2020_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_01_2021_task= PythonOperator(
    task_id="import_load_LISTINGS_01_2021",
    python_callable=import_load_LISTINGS_01_2021_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_02_2021_task= PythonOperator(
    task_id="import_load_LISTINGS_02_2021",
    python_callable=import_load_LISTINGS_02_2021_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_03_2021_task= PythonOperator(
    task_id="import_load_LISTINGS_03_2021",
    python_callable=import_load_LISTINGS_03_2021_func,
    provide_context=True,
    dag=dag
)

import_load_LISTINGS_04_2021_task= PythonOperator(
    task_id="import_load_LISTINGS_04_2021",
    python_callable=import_load_LISTINGS_04_2021_func,
    provide_context=True,
    dag=dag
)
## tasks dependencies
[import_load_NSW_LGA_CODE_task,import_load_NSW_LGA_SUBURB_task] >> import_load_2016Census_G01_NSW_LGA_task 
import_load_2016Census_G01_NSW_LGA_task >> import_load_2016Census_G02_NSW_LGA_task >> import_load_LISTINGS_05_2020_task>>import_load_LISTINGS_06_2020_task>>import_load_LISTINGS_07_2020_task
import_load_LISTINGS_07_2020_task>>import_load_LISTINGS_08_2020_task>>import_load_LISTINGS_09_2020_task>>import_load_LISTINGS_10_2020_task>>import_load_LISTINGS_11_2020_task
import_load_LISTINGS_11_2020_task>>import_load_LISTINGS_12_2020_task>>import_load_LISTINGS_01_2021_task>>import_load_LISTINGS_02_2021_task>>import_load_LISTINGS_03_2021_task
import_load_LISTINGS_03_2021_task>>import_load_LISTINGS_04_2021_task















