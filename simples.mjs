import { PrismaClient } from "@prisma/client";
import cep from "cep-promise";

const MAX_CEP = 99999999;
const batchSize = 300;
const prisma = new PrismaClient();

async function fetchCepInfo(requestedCep) {
  const cepWithLeftPad = requestedCep.toString().padStart(8, "0");
  try {
    await cep(cepWithLeftPad);
    return true;
  } catch (error) {
    if (error.name == "CepPromiseError") {
      //console.log("error", error);
    }

    if (error.type == "validation_error") {
      //console.error("error", error);
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
  const currentCep = 4106292;
  try {
    for (let i = currentCep; i <= MAX_CEP; i += batchSize) {
      const promises = [];
      for (let j = 0; j < batchSize; j++) {
        promises.push(processCEP(i + j));
      }

      await Promise.all(promises);
      console.log("cep", i);
      //setImmediate(global.gc);
    }
  } catch (error) {
    console.error("Error processing CEPs: ", error);
  } finally {
    await prisma.$disconnect();
    process.exit();
  }
}

processCEPs();
