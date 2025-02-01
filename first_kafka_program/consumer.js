import Kafka from 'node-rdkafka';

// Create a consumer instance
const consumer = new Kafka.KafkaConsumer({
  'group.id': 'console-consumer-22844',  // Consumer group ID
  'metadata.broker.list': 'localhost:9092',  // Kafka broker addresses
  'auto.offset.reset': 'earliest',  // Start from the earliest message if no offset is committed
}, {});

consumer.connect();

// On connection, subscribe to the topic
consumer.on('ready', () => {
  console.log('Consumer ready');
  consumer.subscribe(['test']);  // Topic to subscribe to
  consumer.consume();
}).on('data', (message) => {
  // This is where you handle the incoming messages
  console.log(`Received message: ${JSON.parse(message.value.toString())}`);
  // You can parse the message as JSON if it's a JSON message:
  // const messageContent = JSON.parse(message.value.toString());
});

// Error handling
consumer.on('event.error', (err) => {
  console.error('Error in Kafka consumer:', err);
});

// Close the consumer gracefully on termination
process.on('SIGINT', () => {
  consumer.disconnect();
  console.log('Consumer disconnected');
});
