# Big Data project
1. Create "datasets" directory in the root directory
2. Download batch and streaming datasets from these 2 links:
- https://www.kaggle.com/cisautomotiveapi/large-car-dataset
- https://www.kaggle.com/austinreese/craigslist-carstrucks-data
3. Download PostgreSQL JDBC Driver v42.3.0 in jar format for saving processed data to database
- https://jdbc.postgresql.org/download.html

## Batch
1. Go to "batch" directory and start docker-compose
```
docker-compose up --build
```
2. Go to "upload_data" directory and run command below to transfer datasets to HDFS
```
./docker_copy.sh
```
3. Go to Airflow URL http://localhost:8282/ and run "batch-dag"
- All batch queries will be applied to the batch dataset  
- Also, 2 tables will be created for data visualization - "price_decline" and "num_of_ads"
4. Go to Metabase URL http://localhost:3000/ and run SQL queries for data visualization
- Create SQL query below to visualize car model's price decline per year
```
SELECT "brandName", "modelName", "vf_ModelYear", "AvgPrice", "PriceDiff", "TotalCars" from price_decline
WHERE "modelName" = {{modelname}}
GROUP BY "modelName", "vf_ModelYear", "brandName", "AvgPrice", "PriceDiff", "TotalCars"
ORDER BY "vf_ModelYear" desc;
```
- Set "vf_ModelYear" as x-axis and "AvgPrice" for y-axis
