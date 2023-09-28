import { PrismaClient } from "@prisma/client";
import cep from "cep-promise";

//import sqlite3 from "sqlite3";
//import { open } from "sqlite";

const MAX_CEP = 99999999;
const BATCH_SIZE = 1000;

const prisma = new PrismaClient();

const used = process.memoryUsage();

function formatBytes(bytes) {
  const units = ["B", "KB", "MB", "GB"];
  let index = 0;

  while (bytes > 1024 && index < units.length - 1) {
    bytes /= 1024;
    index++;
  }

  return `${bytes.toFixed(2)} ${units[index]}`;
}
/*
async function fetchCepInfo(requestedCep) {
  const cepWithLeftPad = String(requestedCep).padStart(8, "0");
  const url = `https://brasilapi.com.br/api/cep/v1/${cepWithLeftPad}`;
  try{
	const res = await fetch(url);
	return res.ok || res.status !== 404;
  } catch (err) {
	return false
  }
}
*/
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

async function saveToDb(requestedCep) {
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

async function main() {
  for (let i = 113000; i <= MAX_CEP; i += BATCH_SIZE) {
    const tasks = [];
    for (let j = 0; j < BATCH_SIZE; ++j) {
      const requestedCep = i + j;
      tasks.push(
        (async () => {
          const isValid = await fetchCepInfo(requestedCep);
          if (isValid) {
            await saveToDb(requestedCep);
          }
        })()
      );
    }

    // Await all tasks to complete
    await Promise.all(tasks);
    console.log(`cep: ${i}`);
    console.log("Uso de memória:");
    console.log(`- Resident Set Size: ${formatBytes(used.rss)}`);
    console.log(`- Heap Total: ${formatBytes(used.heapTotal)}`);
    console.log(`- Heap Used: ${formatBytes(used.heapUsed)}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
