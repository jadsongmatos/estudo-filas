import amqp from "amqplib";

import cep from 'cep-promise';

//import axios from "axios";
//const API_URL = "https://brasilapi.com.br/api/cep/v2/";
const RABBITMQ_URL = "amqp://localhost";

let channel: any;

async function fetchFromApi(requestedCep: number, retries: number) {
  const paddedCep = requestedCep.toString().padStart(8, "0");

  try {
    await cep(paddedCep);

    return false;
  } catch (error: any) {
    if (error.type === 'service_error') {
      return false;
    }

    if (error.name !== 'CepPromiseError') {
      console.log("error", error)

      if (retries > 0) {
        if (channel && channel.sendToQueue) {
          channel.sendToQueue(
            "searchQueue",
            Buffer.from(JSON.stringify({ cep: cep, retries: retries - 1 }))
          );
        } else {
          setTimeout(() => fetchFromApi(requestedCep, retries), 1000);
        }
      } else {
        console.log("fetchFromApi error", cep, error?.code);
      }
    }

    if (error.type === 'validation_error') {
      console.log("error", error)
      throw error
    }

    return false;
  }
}

async function worker() {
  const connection = await amqp.connect(RABBITMQ_URL);
  channel = await connection.createChannel();
  channel.prefetch(4);
  console.log("worker")
  channel.consume("searchQueue", async (message: any) => {
    const data = JSON.parse(message.content.toString());
    const { cep, retries } = data;
    const response = await fetchFromApi(cep, retries);
    // If API request was successful, add the result to the write queue
    if (response) {
      channel.sendToQueue(
        "writeQueue",
        Buffer.from(JSON.stringify(response))
      );
    }
    channel.ack(message);
  });
}

worker()
