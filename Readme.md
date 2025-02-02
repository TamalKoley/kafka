Run mongodb in localhost:27017 using docker images in WSL
docker run -d --rm -p 27017:27017  mongo
Start zookeper and kafka server in WSL
bin/kafka-server-start.sh config/server.properties
sudo systemctl start zookeeper
post the messages in Kafka using producer service
cosume the messages from kafka using consumer service
consumer service will save the messsages into mongodb that is running in WSL from docker images
consumer service will fetech the messages from  mongodb that is running in WSL from docker images