from airflow.decorators import task, dag
from datetime import datetime, timedelta
import pandas as pd
from supabase import create_client
import json
from random import randint
from scipy import stats
import logging
import numpy as np
import pickle
import random 
import mlflow
from time import strftime
import os


from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor
# from sklearn.linear_model import LinearRegression,Lasso,Ridge,ElasticNet
# load_dotenv()
# url : str= os.getenv('SUPABASE_URL')
# key: str = os.getenv('SUPABASE_KEY')



url = "https://jbmadihsplmgajpaxywf.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpibWFkaWhzcGxtZ2FqcGF4eXdmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MTQxOTMxNjUsImV4cCI6MjAyOTc2OTE2NX0.QgyfO_jrqNfY7_ZOm6KnEb4BrmUsj-wumP3DuqrieOM"

def init():
    supabase = create_client(url, key)
    return supabase


def save_object(file_path, obj):
      try: 
         dir_path = os.path.dirname(file_path)
         os.makedirs(dir_path, exist_ok= True)
         with open(file_path, 'wb') as file_obj:
            pickle.dump(obj, file_obj)
      except Exception as e:
         print(e)



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
        else: 
            return district["streets"][randint(0, len(district['streets'])-1)]
    return None
        
def get_random_ward():
    district = random.choice(location["district"])
    if district['wards']:
        return np.random.choice(district['wards'])
    else:
        return "Lò Đúc"
def get_random_street():
    district = random.choice(location["district"])
    if district['streets']:
        return np.random.choice(district['streets'])
    else:
        return "Phố Lò Đúc"
def get_random_district():
    return np.random.choice(location["district"])['name']

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
        for index, row in df.iterrows():
        # get district by ward if none return random district
            if row['district'] == '' or pd.isnull(row['district']):
                if row['street'] == '':
                    # get random district 
                    row['district'] = get_random_district()
                else: 
                    ward = row['ward']
                    df.at[index, 'district'] = get_district_name_by_ward(location, ward)

        # get ward by street if none return random ward
            if row['ward'] == '' or pd.isnull(row['ward']):
                if row['street'] == '':
                    # get random ward 
                    row['ward'] = get_random_ward()
                else:
                    street = row['street']
                    df.at[index, 'ward'] = get_ward_by_street(location, street)

        # get street by ward if none return random street
            if row['street'] == '' or pd.isnull(row['street']):
                if row['ward'] == '':
                    row['street'] = get_random_street()
                else: 
                    ward = row['ward']
                    df.at[index, 'street'] = get_street_by_ward(location, ward)
            if row['direction'] == '':
                df.at[index, 'direction'] = 0

        df = df.dropna()
        df = df.reset_index(drop=True)
        # remove outliers
        df['price_zscore'] = stats.zscore(df['price'])
        df['area_zscore'] = stats.zscore(df['area'])
        logging.info('sao no lai khong chay')
        price_outlier = df[(df['price_zscore'].abs() > 0.3)]
        area_outlier = df[(df['area_zscore'].abs() > 1.36)]
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
    def train_model(df_json):
        df = pd.read_json(df_json, orient='split')
        # drop unnecessary columns
        df = df.drop(columns=['id', 'created_at', 'post_date', 'current_floor', 'num_floor', 'direction', 'street_width', 'price_zscore', 'area_zscore'])
        categorical_cols = ['street', 'ward', 'district']
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
        models = {
            # "LinearRegression": LinearRegression(),
            # "Lasso": Lasso(),
            # "Ridge": Ridge(),
            # "ElasticNet": ElasticNet(),
            "RandomForestRegressor": RandomForestRegressor()
        }

        # Split the data into training and testing sets
        X = df.drop(columns='price')
        y = df['price']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1)

        # Iterate over each model, train it, and evaluate its performance
        for model_name, model in models.items():
            clf = Pipeline(steps=[('preprocessor', preprocessor),
                                ('model', model)])
            clf.fit(X_train, y_train)
            y_pred = clf.predict(X_test)            
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            time = strftime("%Y-%m-%d")
            with open(f'plugins/model/model_{time}.pkl','wb') as file:
                pickle.dump(clf, file)
            with open(f'plugins/model/rmse.txt','w') as file:
                file.write(str(rmse))
            return float(rmse)

    @task()
    def eval_model(rmse:float):
        print(rmse)

    
    data = fetch_data()
    processed_data = process_data(data)
    train_model_task = train_model(processed_data)
    eval_model(train_model_task)

    # Set task dependencies
    # data >> processed_data >> train_model_task >> eval_model_task   
    


predict_price_house()

