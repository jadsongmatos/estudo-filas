import https from "https";
import { PrismaClient } from "@prisma/client";
//import cep from "cep-promise";
//const process = import.meta.require('process');

const MAX_CEP = 99999999;
const batchSize = 1000;
const prisma = new PrismaClient();
var keepAliveAgent = new https.Agent({ keepAlive: true });

const options = {
  method: "GET",
  mode: "cors",
  headers: {
    "content-type": "application/json;charset=utf-8",
  },
  timeout: 600,
  agent: keepAliveAgent,
};

async function fetchCepInfo(requestedCep) {
  const cepWithLeftPad = requestedCep.toString().padStart(8, "0");
  const url = `https://brasilapi.com.br/api/cep/v1/${cepWithLeftPad}`;

  try {
    const res = await fetch(url, options);
    return res.ok || res.status !== 404;
  } catch (error) {
    if (
      error.cause.code != "ECONNRESET" &&
      error.cause.code !== "ETIMEDOUT" &&
      error.cause.code !== "UND_ERR_CONNECT_TIMEOUT"
    ) {
      console.error(error.cause);
    }
    return false;
  }
}

async function processCEP(requestedCep) {
  try {
    const isValid = await fetchCepInfo(requestedCep);
    if (isValid) {
      try {
        await prisma.ceps.create({
          data: { cep: requestedCep },
        });
        console.log("W", requestedCep);
      } catch (error) {
        if (error.code !== "P2002") {
          console.error("Prisma error: ", error);
        }
      }
    }
  } catch (error) {
    console.error("Error: ", error);
  }
}

process.on("SIGINT", function () {
  console.log("\nSalvando...");
  prisma.$disconnect().finally(() => {
    process.exit();
  });
});

async function processCEPs() {
  const currentCep = 7398535;
  try {
    for (let i = currentCep; i <= MAX_CEP; i += batchSize - 1) {
      const promises = Array.from({ length: batchSize }, (_, j) =>
        processCEP(i + j)
      );
      await Promise.all(promises);
      console.log("cep", i);
      keepAliveAgent = new https.Agent({ keepAlive: true });
    }
  } catch (error) {
    console.error("Error processing CEPs: ", error);
  } finally {
    await prisma.$disconnect();
    process.exit();
  }
}

processCEPs();
