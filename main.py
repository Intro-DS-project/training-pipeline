from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
import mlflow
import pandas as pd
import json
import pickle

app = FastAPI()

path = 'plugins/model/model_2024-05-25.pkl'

class InputData(BaseModel):
    area: int
    street: str
    ward: str
    district: str
    num_bedroom: int
    num_diningroom: int
    num_kitchen: int
    num_toilet: int

checkpoint = {}

with open('dags/location.json') as f:
    location = json.load(f)

def validate_input_data(input_data: InputData):
    if input_data.area < 0 or input_data.num_bedroom < 0 or input_data.num_diningroom < 0 or input_data.num_kitchen < 0 or input_data.num_toilet < 0:
        raise HTTPException(status_code=400, detail="All numeric values must be non-negative.")
    
    district_names = [district['name'] for district in location['district']]
    if input_data.district not in district_names:
        raise HTTPException(status_code=400, detail="Invalid district name.")
    
    district = next((district for district in location['district'] if district['name'] == input_data.district), None)
    if input_data.ward not in district['wards']:
        raise HTTPException(status_code=400, detail="Invalid ward name for the given district.")
    
    if input_data.street not in district['streets']:
        raise HTTPException(status_code=400, detail="Invalid street name for the given district and ward.")

@app.on_event("startup")
async def load_model():
    print("Loading model...")
    with open(path, 'rb') as file:
        model = pickle.load(file)
    checkpoint['model'] = model
    print("Model loaded.")

@app.on_event("shutdown")
async def unload_model():
    print("Unloading model...")
    checkpoint.clear()

@app.post("/predict")
async def predict(input_data: InputData):
    validate_input_data(input_data)
    
    model = checkpoint.get('model')
    if model is None:
        raise HTTPException(status_code=500, detail="Model is not loaded.")
    
    input_df = pd.DataFrame([input_data.dict()])
    
    prediction = model.predict(input_df)[0]
    
    return {"predicted_price": prediction}
