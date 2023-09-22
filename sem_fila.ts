import { PrismaClient } from "@prisma/client";
import cep from 'cep-promise';

const MAX_CEP = 99999999;
const PARALLEL_REQUESTS = 1024; // Quantidade de solicitações simultâneas
let prisma = new PrismaClient();

async function writeToDB(cep: number) {
  console.log("W", cep);
  try {
    await prisma.ceps.create({
      data: {
        cep
      }
    })
  } catch (error: any) {
    if (error.code !== 'P2002') {
      console.log("prisma error", error);
    }
  }
}

async function fetchFromApi(requestedCep: number, retries: number): Promise<boolean> {
  const paddedCep = requestedCep.toString().padStart(8, "0");

  try {
    await cep(paddedCep);
    return true;
  } catch (error: any) {
    if (error.type === 'service_error') {
      return false;
    }
    if (error.name !== 'CepPromiseError') {
      if (retries > 0) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        return fetchFromApi(requestedCep, retries - 1);
      } else {
        console.log("fetchFromApi error", paddedCep, error.type);
        return false;
      }
    }

    if (error.type === 'validation_error') {
      console.log("error", error);
      throw error;
    }

    return false;
  }
}

function getStartingZip() {
  return 5938530;
}

async function processChunk(ceps: number[]): Promise<void> {
  const promises: Promise<void>[] = [];

  for (const cep of ceps) {
    promises.push(
      fetchFromApi(cep, 3).then(response => {
        if (response) {
          return writeToDB(cep);
        }
      })
    );
  }

  await Promise.all(promises);
}

let currentCep = getStartingZip();
let chunk = new Array(PARALLEL_REQUESTS)

async function processCEPs(currentCep:number) {
  if (currentCep <= MAX_CEP) {
    for (let j = 0; j < PARALLEL_REQUESTS; j++) {
      const requestedCep = currentCep + j
      chunk[j] = fetchFromApi(requestedCep, 3).then(response => {
        if (response) {
          return writeToDB(requestedCep);
        }
      })
    }
    console.log("cep", currentCep)
    await Promise.all(chunk);
    processCEPs(currentCep += PARALLEL_REQUESTS)
  } else {
    process.exit(0);
  }
}

processCEPs(currentCep);