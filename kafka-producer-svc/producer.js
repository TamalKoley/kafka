
import Kafka from 'node-rdkafka';
// Create a Kafka producer instance

console.log("program is running");
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:9092';
const producer = new Kafka.Producer({
  'metadata.broker.list': kafkaBroker,  // Replace with your Kafka broker address
  'dr_cb': true,  // Delivery report callback
});

const jsonPayload1=JSON.stringify({
  "Name" : "Subham Bose",
  "Age" : "28",
  "Designation" : "Software Developer",
  "Company" : "CTS"
});
const jsonPayload2=JSON.stringify({
  "Name" : "Subham Bose",
  "Age" : "28",
  "Designation" : "Data Scientist",
  "Company" : "CTS"
});
 const payloadArray=[jsonPayload1,jsonPayload2];


// Event handler for ready producer
producer.on('ready', () => {
  console.log('Producer is ready');
  try {
    // Produce a message to the 'test' topic
    payloadArray.map((payload)=>{
    producer.produce(
      'test',  // Kafka topic name
      null,    // Partition (null means auto)
      Buffer.from(payload),  // Message payload
      null,    // Key (optional)
      Date.now()  // Timestamp (optional)
    );
    console.log('Message sent');
  });
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
