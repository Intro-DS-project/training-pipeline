# training-pipeline

## Overview
Predict the price of rental rooms in Hanoi


## Getting started
1. Clone the repository
``` bash 
git clone https://github.com/Intro-DS-project/training-pipeline.git
```
2. In the root of repository install dependencies
```bash
cd training-pipeline
pip install -r requirements.txt
pip install -r requirements_api.txt
```
3. Run predict api 
```bash 
uvicorn main:app --host 0.0.0.0 --port 8000   
```
Access the api  in your web browser at http://localhost:8000.
- Api endpoint: http://0.0.0.0:8000/predict

- example : 
```
{
  "area": 30,
  "num_bedroom": 0,
  "num_diningroom": 0,
  "num_kitchen": 0,
  "num_toilet": 0,
  "street": "Đại La",
  "ward": "Trương Định",
  "district": "Hai Bà Trưng"
}
```
- reponse :
```
{
  "predicted_price": 3.516000000000001
}
```
4. Run Apache Airflow DAGs: Start the Airflow web server
```bash 
airflow webserver
```
- Access the Airflow UI in your web browser at http://localhost:8080.

5. Run with docker 
- If you prefer to use Docker, you can easily run the project with Docker Compose. Simply navigate to the project directory and run:
```bash 
docker-compose up --build
```

- and then run :
```bash 
docker-compose up
```
