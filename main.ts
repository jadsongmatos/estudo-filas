import amqp from 'amqplib';
import axios from "axios";
import { PrismaClient } from "@prisma/client";

const MAX_CEP = 99999999;
const API_URL = "https://brasilapi.com.br/api/cep/v2/";
const RABBITMQ_URL = "amqp://localhost";

let channel: any;
let prisma = new PrismaClient();

// Function to read the ceps.bin file and get the biggest zip code
async function getStartingZip() {
  return 2110030
}

async function fetchFromApi(cep: number, retries: number) {
  const paddedCep = cep.toString().padStart(8, "0");

  try {
    const response = await axios(`${API_URL}${paddedCep}`);
    // Successful request
    if (response.status !== 404) {
      console.log("S", cep)
      return cep
    }
    return false
  } catch (error: any) {
    // Handle errors
    if (error.response && error.response.status === 404) {
      return false
    } else {
      if (retries > 0) {
        channel.sendToQueue('searchQueue', Buffer.from(JSON.stringify({ cep: cep, retries: retries - 1 })));
      } else {
        console.log('axios error', cep, error?.code)
      }
      return false
    }
  }
}

// Add to queue for search
async function addToSearchQueue() {
  let cep = await getStartingZip();
  console.log("Starting", cep);

  // Recursive function to add CEPs to the queue
  const addNextCepToQueue = () => {
    if (cep < MAX_CEP) {
      channel.sendToQueue('searchQueue', Buffer.from(JSON.stringify({ cep: cep, retries: 3 })));
      if (cep % 10000 == 0) {
        console.log("CEP", cep)
      }
      cep++;
      setImmediate(addNextCepToQueue); // Call the next CEP in the next iteration of the event loop
    }
  };

  // Start adding CEPs to the queue
  addNextCepToQueue();
}


// Consume the search queue, fetch data from API and add to write queue
async function consumeSearchQueue() {
  const CONSUMER_COUNT = 10;

  for (let i = 0; i < CONSUMER_COUNT; i++) {
    channel.consume('searchQueue', async (message: any) => {
      const { cep, retries } = JSON.parse(message.content.toString());
      const response = await fetchFromApi(cep, retries);

      // If API request was successful, add the result to the write queue
      if (response) {
        channel.sendToQueue('writeQueue', Buffer.from(JSON.stringify(response)));
      }
      channel.ack(message);
    });
  }
}

// Consume the write queue and write to database
async function consumeWriteQueue() {
  const CONSUMER_COUNT = 10;
  let batch: Array<any> = []

  for (let i = 0; i < CONSUMER_COUNT; i++) {
    channel.consume('writeQueue', async (message: any) => {
      const cep: number = JSON.parse(message.content.toString());
      console.log("W", cep)
      batch.push(prisma.ceps.create({ data: { cep: cep } }));

      if (batch.length >= 128) {
        try {
          await prisma.$transaction(batch);
          console.log("Batch written")
        } catch (error: any) {
          console.log('prisma error', error)
        }
        batch = [];
      }

      const queueInfo = await channel.assertQueue('writeQueue');
      if (queueInfo.messageCount === 0 && batch.length > 0) {
        try {
          await prisma.$transaction(batch);
          console.log("Remaining batch written")
        } catch (error: any) {
          console.log('prisma error', error)
        }
        batch = [];
      }

      channel.ack(message);
    });
  }
}


// Main function to start the script
async function main() {
  const connection = await amqp.connect(RABBITMQ_URL);
  channel = await connection.createChannel();

  await channel.assertQueue('searchQueue');
  await channel.assertQueue('writeQueue');

  // Start consuming and adding to the queue in parallel
  await Promise.all([addToSearchQueue(), consumeSearchQueue(), consumeWriteQueue()]);
}

main().catch(console.error);
