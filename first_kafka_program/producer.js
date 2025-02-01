
import Kafka from 'node-rdkafka';
// Create a Kafka producer instance
const producer = new Kafka.Producer({
  'metadata.broker.list': 'localhost:9092',  // Replace with your Kafka broker address
  'dr_cb': true,  // Delivery report callback
});

const jsonPayload=JSON.stringify({
  "Name" : "Tamal Koley",
  "Age" : "28",
  "Designation" : "Software Developer"
});

// Event handler for ready producer
producer.on('ready', () => {
  console.log('Producer is ready');
  try {
    // Produce a message to the 'test' topic
    producer.produce(
      'test',  // Kafka topic name
      null,    // Partition (null means auto)
      Buffer.from(jsonPayload),  // Message payload
      null,    // Key (optional)
      Date.now()  // Timestamp (optional)
    );
    console.log('Message sent');
    producer.disconnect(() => {
       
      });
  } catch (err) {
    console.error('Error producing message:', err);
  }
});

// Event handler for errors
producer.on('event.error', (err) => {
  console.error('Error in producer:', err);
});

// Connect the producer
producer.connect();

// Handle disconnect
producer.on('disconnected', () => {
  console.log('Producer disconnected');
});
