docker cp hdfs_upload.sh namenode:/hdfs_upload.sh

cd ../datasets

docker cp postgresql-42.3.0.jar spark-master:/postgresql-42.3.0.jar

echo "Copying data to namenode container..."
docker cp CIS_Automotive_Kaggle_Sample.csv namenode:/batch-dataset.csv
docker cp illinois-children.csv namenode:/illinois-children.csv
docker cp ads-streaming.csv namenode:/ads-streaming.csv
docker cp ads-join-streaming.csv namenode:/ads-join-streaming.csv

echo "Entering the namenode and starting the upload to HDFS process..."
docker exec -it namenode bash ./hdfs_upload.sh