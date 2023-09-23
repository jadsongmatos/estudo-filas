import { PrismaClient } from "@prisma/client";
import cep from "cep-promise";
//const process = import.meta.require('process');

const MAX_CEP = 99999999;
const batchSize = 1000;
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
  const currentCep = 7437496;
  try {
    for (let i = currentCep; i <= MAX_CEP; i += batchSize - 1) {
      const promises = Array.from({ length: batchSize }, (_, j) =>
        processCEP(i + j)
      );
      await Promise.all(promises);
      console.log("cep", i);
    }
  } catch (error) {
    console.error("Error processing CEPs: ", error);
  } finally {
    await prisma.$disconnect();
    process.exit();
  }
}

processCEPs();
