import { MongoClient } from 'mongodb';

const url= 'mongodb://localhost:27017';
const dbName='MyProjectDB1';
const collectionName='KafkaTopicData';


// Connect to MongoDB



export const insertIntoMongoDB = async (message) => {
    const client = new MongoClient(url, { useNewUrlParser: true, useUnifiedTopology: true });
  
    try {
      // Connect to MongoDB
      await client.connect();
      console.log('Connected to MongoDB');
  
      // Create database and collection if they do not exist
      const db = client.db(dbName);
      const collection = db.collection(collectionName);
  
      // Insert the message into the collection
      await collection.insertOne({ message });
      console.log('Message inserted into MongoDB');
    } catch (err) {
      console.error('Error connecting to MongoDB or inserting document:', err);
    } finally {
      // Ensure the client is closed
      await client.close();
    }
  };

  // Function to retrieve data from MongoDB
export const getDataFromMongoDB = async () => {
    const client = new MongoClient(url, { useNewUrlParser: true, useUnifiedTopology: true });
  
    try {
      // Connect to MongoDB
      await client.connect();
      console.log('Connected to MongoDB');
  
      // Access the database and collection
      const db = client.db(dbName);
      const collection = db.collection(collectionName);
  
      // Retrieve data from the collection
      const data = await collection.find({}).toArray();
      //console.log('Data retrieved from MongoDB:', data);
  
      return data;
    } catch (err) {
      console.error('Error connecting to MongoDB or retrieving data:', err);
    } finally {
      // Ensure the client is closed
      await client.close();
    }
  };