cd ../druid
docker-compose up --build &

cd ..
docker-compose up --build &

# app stopping on ctrl+c
function trap_ctrlc(){
	echo "stopping..."
	./docker-down.sh
}

trap "trap_ctrlc" INT

wait