import amqp from "amqplib";
import { PrismaClient } from "@prisma/client";
import { Worker } from 'worker_threads';

const MAX_CEP = 99999999;
const RABBITMQ_URL = "amqp://localhost";

let channel: any;
let prisma = new PrismaClient();
let connection: any;
let load = true

for (let i = 0; i < 32; i++) {
  const worker = new Worker('./worker-script.ts');

  worker.on('message', (msg) => {
    if (msg.error) {
      console.error(msg.error);
    }
  });

  worker.on('error', (err) => {
    console.error(err);
  });

  worker.on('exit', (code) => {
    if (code !== 0) {
      console.error(`Worker stopped with exit code ${code}`);
    }
  });
}

async function consumeWriteQueue() {
  channel.consume("writeQueue", async (message: any) => {
    const data = JSON.parse(message.content.toString());
    const cep: number = data;
    console.log("W", cep)

    try {
      await prisma.ceps.create({
        data: {
          cep
        }
      })
    } catch (error: any) {
      // Checa se o erro é um erro de chave primária duplicada
      if (error.code != 'P2002') {
        console.log("prisma error", error);
      }
    }
    channel.ack(message);
  });
}

async function createChannel() {
  try {
    channel = await connection.createChannel();
    channel.on("close", () => {
      console.error("RabbitMQ channel closed, recreating...");
      setTimeout(createChannel, 1000);
    });

    channel.on("error", (error: any) => {
      if (error.message !== "Channel closing") {
        console.error("[AMQP] channel error", error.message);
      }
    });

    await channel.assertQueue("searchQueue");
    await channel.assertQueue("writeQueue");

    // Start consuming and adding to the queue in parallel
    await Promise.all([
      addToSearchQueue(),
      consumeWriteQueue(),
    ]);

    return new Promise((resolve, reject) => {
      const intervalId = setInterval(async () => {
        const searchQueue = await channel.checkQueue("searchQueue");
        const writeQueue = await channel.checkQueue("writeQueue");
        if (searchQueue.messageCount === 0 && writeQueue.messageCount === 0 && load == false) {
          clearInterval(intervalId);
          resolve(true);
        }
      }, 1000);
    });

  } catch (error) {
    console.error("Failed to create a RabbitMQ channel, retrying...", error);
    setTimeout(createChannel, 1000);
  }
}

async function connectRabbitMQ() {
  try {
    connection = await amqp.connect(RABBITMQ_URL);

    connection.on("close", () => {
      console.error("RabbitMQ connection closed, reconnecting...");
      setTimeout(connectRabbitMQ, 1000);
    });

    connection.on("error", (error: any) => {
      if (error.message !== "Connection closing") {
        console.error("[AMQP] conn error", error.message);
      }
    });

    connection.on("close", function () {
      console.error("[AMQP] reconnecting");
      return setTimeout(connectRabbitMQ, 1000);
    });

    createChannel()
      .then(() => {
        // Close the connection and exit the process
        connection.close();
        console.log("All messages have been processed, exiting...");
        process.exit(0);
      })
      .catch((error) => {
        console.error("Failed to process all messages, exiting...", error);
        process.exit(1);
      });
  } catch (error) {
    console.error("Failed to connect to RabbitMQ, retrying...", error);
    setTimeout(connectRabbitMQ, 1000);
  }
}

connectRabbitMQ();

async function getStartingZip() {
  return 5820000
}

async function addToSearchQueue() {
  let cep = await getStartingZip();
  console.log("Starting", cep);

  const bigLoop = setInterval(() => {
    if (cep >= MAX_CEP) {
      clearInterval(bigLoop);
      load = false
    } else {
      if (cep % 10000 == 0) {
        console.log("cep", cep);
        if (global.gc) {
          global.gc();
        }
      }
      channel.sendToQueue(
        "searchQueue",
        Buffer.from(JSON.stringify({ cep: cep, retries: 3 }))
      );
      cep++;
    }
  }, 10);
}
