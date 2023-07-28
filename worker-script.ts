import amqp from "amqplib";
import axios from "axios";

const API_URL = "https://brasilapi.com.br/api/cep/v2/";
const RABBITMQ_URL = "amqp://localhost";

let channel: any;

async function fetchFromApi(cep: number, retries: number) {
  const paddedCep = cep.toString().padStart(8, "0");
  try {
    const response = await axios.get(`${API_URL}${paddedCep}`);
    // Successful request
    if (response.status !== 404) {
      return cep;
    }
    return false;
  } catch (error: any) {
    // Handle errors
    if (error.response && error.response.status === 404) {
      return false;
    } else {
      if (retries > 0) {
        if (channel && channel.sendToQueue) {
          channel.sendToQueue(
            "searchQueue",
            Buffer.from(JSON.stringify({ cep: cep, retries: retries - 1 }))
          );
        } else {
          setTimeout(() => fetchFromApi(cep, retries), 1000);
        }
      } else {
        console.log("axios error", cep, error?.code);
      }
      return false;
    }
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
