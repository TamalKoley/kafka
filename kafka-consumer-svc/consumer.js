import Kafka from 'node-rdkafka';
import {insertIntoMongoDB,getDataFromMongoDB} from './MongoDB.js';



// Create a consumer instance
const consumer = new Kafka.KafkaConsumer({
  'group.id': 'console-consumer-22844',  // Consumer group ID
  'metadata.broker.list': 'localhost:9092',  // Kafka broker addresses
  'auto.offset.reset': 'earliest',  // Start from the earliest message if no offset is committed
}, {});


consumer.connect();

const disconnectConsumer = () => {
  consumer.disconnect();
  console.log('Consumer disconnected');
  console.log('fecthing data from Mongo DB...');
  getDataFromMongoDB().then((data)=>{
  console.log('Data retrieved from MongoDB:',data);
});
};

let consumerTimer
const resetTimer = () => {
  if (consumerTimer) {
    clearTimeout(consumerTimer);
  }
  consumerTimer = setTimeout(disconnectConsumer, 10000);
};

// On connection, subscribe to the topic
consumer.on('ready', () => {
  console.log('Consumer ready');
  consumer.subscribe(['test']);  // Topic to subscribe to
  consumer.consume();
}).on('data', async (message) => {
  // This is where you handle the incoming messages
  console.log(`Received message: ${(message.value.toString())}`);
  resetTimer();
  // You can parse the message as JSON if it's a JSON message:
  // const messageContent = JSON.parse(message.value.toString());
  // insert into DB
  const document=JSON.parse(message.value.toString());
  await insertIntoMongoDB(document);

});


// Error handling
consumer.on('event.error', (err) => {
  console.error('Error in Kafka consumer:', err);
});

// Close the consumer gracefully on termination
process.on('SIGINT', () => {
  clearTimeout(consumerTimer);
  disconnectConsumer();
});

//retrieve data from MongoDB
