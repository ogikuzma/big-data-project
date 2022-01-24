docker cp ./spark-jobs.sh spark-master:./spark-jobs.sh
docker exec -it spark-master bash ./spark-jobs.sh