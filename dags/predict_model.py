from airflow.models import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
import json
from random import randint
from scipy import stats
import logging
import numpy as np
import pickle

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression,Lasso,Ridge,ElasticNet

# dotenv_path = Path('.env')
# load_dotenv(dotenv_path=dotenv_path)

# url: str = os.getenv('SUPABASE_URL')
# key: str = os.getenv('SUPABASE_KEY')
url = "https://jbmadihsplmgajpaxywf.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpibWFkaWhzcGxtZ2FqcGF4eXdmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MTQxOTMxNjUsImV4cCI6MjAyOTc2OTE2NX0.QgyfO_jrqNfY7_ZOm6KnEb4BrmUsj-wumP3DuqrieOM"

def init():
    supabase = create_client(url, key)
    return supabase


# import os 
# credentialPath = "location.json"
# print(os.path.isfile(credentialPath))


with open('/opt/airflow/dags/location.json', 'r', encoding='utf-8') as file:
    location = json.load(file)

def get_district_name_by_ward(location, ward_name):
    for district in location["district"]:
        if ward_name in district["wards"]:
            return district["name"]
    return None 

def get_ward_by_street(location, street_name):
    # Duyệt qua các district trong location
    for district in location["district"]:
        if street_name in district["streets"]:
            index = district["streets"].index(street_name)
            if index < len(district["wards"]):
                return district["wards"][index]
    return None  

def get_street_by_ward(location, ward):
    for district in location["district"]:
        if ward in district["wards"]:
            index = len(district['streets'])
            return district["streets"][randint(0, index-1)]
        

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 20),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

@dag(default_args=default_args, schedule='@daily', catchup=False, dag_id='predict_price_house')
def predict_price_house():
    # @task 
    # def start():
    #      SUPABASE_URL= "https://jbmadihsplmgajpaxywf.supabase.co"
    #      SUPABASE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpibWFkaWhzcGxtZ2FqcGF4eXdmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MTQxOTMxNjUsImV4cCI6MjAyOTc2OTE2NX0.QgyfO_jrqNfY7_ZOm6KnEb4BrmUsj-wumP3DuqrieOM"
    #      supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    #      return supabase
    
    @task()
    def fetch_data():
            supabase = init()
            response = supabase.table('muaban').select("*").execute()
            df_muaban = pd.DataFrame(response.data)
            response = supabase.table('mogi').select("*").execute()
            df_mogi = pd.DataFrame(response.data)
            response= supabase.table('rongbay').select("*").execute()
            df_rongbay = pd.DataFrame(response.data)
            df_concatenated  = pd.concat([df_mogi, df_muaban, df_rongbay], ignore_index=True)
            df_concatenated['id'] = range(1, len(df_concatenated) + 1)
            # log to check the data
            # logging.info(f'First few rows of concatenated DataFrame:\n{df_concatenated.head()}')
            # logging.info('jjjjjjjjjjjjjjjjjjjjjjjjjjjjj')
            #r return df_concatenated   # but not return data frame
            df_json = df_concatenated.to_json(orient='split')        
            return df_json


    @task()
    def process_data(df_json):
      # standardize the data
      df = pd.read_json(df_json, orient='split')
      for i in range(len(df)):
            if df.loc[i, 'district'] == '':
                  ward = df.loc[i, 'ward']
                  df.loc[i, 'district'] = get_district_name_by_ward(location, ward)

            if df.loc[i,'ward'] == '':
                  street = df.loc[i, 'street']
                  df.loc[i,'street'] = get_ward_by_street(location, street)

            if df.loc[i,'street'] == '':
                  ward= df.loc[i,'ward']
                  df.loc[i,'street'] = get_street_by_ward(location, ward)
            if df.loc[i, 'direction'] == '':
                  df.loc[i, 'direction'] = 0
            if df.loc[i,'price'] == 0.0:
                  df = df.drop(i)
      # remove outliers
      df['price_zscore'] = stats.zscore(df['price'])
      df['area_zscore'] = stats.zscore(df['area'])
      logging.info('sao no lai khong chay')
      price_outlier = df[(df['price_zscore'].abs() > 3)]
      area_outlier = df[(df['area_zscore'].abs() > 3)]
      logging.info('sao no lai khong chay- ---------')
      outlier_zscore = pd.concat([price_outlier, area_outlier]).drop_duplicates()
      df = df.drop(outlier_zscore.index)      
      logging.info('chay roi thi tra ve di ---------') 
      logging.info(f'First few rows of processed DataFrame:\n{df.head()}')
      categorical_cols  = ['street', 'ward', 'district', 'direction']
      for col in categorical_cols:
            df[col] = df[col].astype('str')

      

      return df.to_json(orient='split')

    @task()
    def train_model(df_json, **kwargs):
        ti = kwargs['ti']
        df = pd.read_json(df_json, orient='split')
        # drop unnecessary columns
        df = df.drop(columns=['id', 'created_at', 'post_date'])
        categorical_cols = ['street', 'ward', 'district', 'direction']
        numerical_cols = df.drop(columns=['price'] + categorical_cols).columns.tolist()
        numerical_transformer = SimpleImputer(strategy='mean')
        categorical_transformer = Pipeline(steps=[
                  ('imputer', SimpleImputer(strategy='most_frequent')),
                  ('onehot', OneHotEncoder(handle_unknown='ignore'))
        ]) 
        
        preprocessor = ColumnTransformer(
            transformers=[
                  ('num', numerical_transformer, numerical_cols),
                  ('cat', categorical_transformer, categorical_cols)
             ])
        clf = Pipeline(steps=[('preprocessor', preprocessor),
                              ('classifier', LinearRegression())])
        X = df.drop(columns='price')
        y = df['price']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)

        # Serialize y_test and y_pred using pickle
        with open('/tmp/y_test.pkl', 'wb') as f:
            pickle.dump(y_test, f)
        with open('/tmp/y_pred.pkl', 'wb') as f:
            pickle.dump(y_pred, f)

    @task()
    def eval_model(**kwargs):
        # Load y_test and y_pred from files
        with open('/tmp/y_test.pkl', 'rb') as f:
            y_test = pickle.load(f)
        with open('/tmp/y_pred.pkl', 'rb') as f:
            y_pred = pickle.load(f)
            
        logging.info(f'y_test: {y_test}')
        logging.info(f'y_pred: {y_pred}')
        
        # Ensure y_test and y_pred are in the correct format
        y_test = np.array(y_test, copy=True)
        y_pred = np.array(y_pred, copy=True)
        mae = mean_absolute_error(y_test, y_pred)
        logging.info(f'Mean Absolute Error: {mae}')

    # Defining the task dependencies
    
    data = fetch_data()
    processed_data = process_data(data)
    train_model_task = train_model(processed_data)
    eval_model_task = eval_model()

    # Set task dependencies
    data >> processed_data >> train_model_task >> eval_model_task   
    


predict_price_house()

