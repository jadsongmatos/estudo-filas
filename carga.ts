import amqp from "amqplib";

const MAX_CEP = 99999999;
const RABBITMQ_URL = "amqp://localhost";
const CONSUMER_COUNT = 256;
//const MAX_QUEUE_SIZE = 99999999

let channel: any;
let connection: any;
let load = true

async function getStartingZip() {
  return 0
}

async function addToSearchQueue() {
  let cep = await getStartingZip();
  console.log("Starting", cep);

  const bigLoop = setInterval(() => {
    if (cep >= MAX_CEP) {
      clearInterval(bigLoop);
    } else {
      if (cep % 10000 == 0) {
        console.log("cep", cep);
      }
      channel.publish(
        "",
        "test",
        Buffer.from(JSON.stringify({ cep: cep, retries: 3 })),
        { noWait: true }
      )
      cep++;
    }
  }, 0);
}



async function createChannel() {
  channel = await connection.createChannel();
  channel.prefetch(CONSUMER_COUNT);
  await channel.assertQueue("test");

  // Start consuming and adding to the queue in parallel
  await Promise.all([
    addToSearchQueue(),
  ]);

  return new Promise((resolve, reject) => {
    const intervalId = setInterval(async () => {
      const searchQueue = await channel.checkQueue("test");
      if (searchQueue.messageCount === 0 && load == false) {
        clearInterval(intervalId);
        resolve(true);
      }
    }, 1000);
  });
}

async function connectRabbitMQ() {

  connection = await amqp.connect(RABBITMQ_URL);

  connection.on("close", () => {
    console.error("RabbitMQ connection closed, reconnecting...");
    setTimeout(connectRabbitMQ, 1000);
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

}

connectRabbitMQ();
