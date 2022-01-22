hdfs dfs -mkdir -p /user/root/data-lake/raw
hdfs dfs -mkdir -p /user/root/data-lake/transformation

hdfs dfs -copyFromLocal -f ./batch-dataset.csv /user/root/data-lake/raw/batch-dataset.csv
hdfs dfs -copyFromLocal -f ./illinois-children.csv /user/root/data-lake/raw/illinois-children.csv
hdfs dfs -copyFromLocal -f ./vehicles.csv /user/root/data-lake/raw/streaming-dataset.csv