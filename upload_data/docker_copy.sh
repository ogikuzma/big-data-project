docker cp upload_data.sh namenode:/upload_data.sh

cd ../datasets

docker cp postgresql-42.3.0.jar spark-master:/postgresql-42.3.0.jar

echo "Copying data to namenode container..."
docker cp CIS_Automotive_Kaggle_Sample.csv namenode:/batch-dataset.csv
docker cp illinois-children.csv namenode:/illinois-children.csv
docker cp vehicles.csv namenode:/vehicles.csv

echo "Entering the namenode and starting the upload to HDFS process..."
docker exec -it namenode bash ./upload_data.sh