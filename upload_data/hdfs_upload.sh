hdfs dfs -mkdir -p /user/root/data-lake/raw
hdfs dfs -mkdir -p /user/root/data-lake/transformation

hdfs dfs -copyFromLocal -f ./batch-dataset.csv /user/root/data-lake/raw/batch-dataset.csv
hdfs dfs -copyFromLocal -f ./illinois-children.csv /user/root/data-lake/raw/illinois-children.csv
hdfs dfs -copyFromLocal -f ./ads-streaming.csv /user/root/data-lake/raw/streaming-dataset.csv
hdfs dfs -copyFromLocal -f ./ads-join-streaming.csv /user/root/data-lake/raw/streaming-join-dataset.csv