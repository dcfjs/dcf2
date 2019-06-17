#!/usr/bin/env node
import {
  autoRelease,
  waitForExitSignal,
  releaseAll,
} from '@dcfjs/common/autoRelease';
import { createWorkerServer } from '.';

// default to random port.
const PORT = (process.env['PORT'] as any | 0) || undefined;
const MASTER_ENDPOINT =
  process.env['MASTER_ENDPOINT'] || 'http://localhost:9001';
const WORKER_SECRET = process.env['WORKER_SECRET'] || 'NO_SECRET';
const HOST = process.env['HOST'] || 'localhost';

async function main() {
  try {
    // Initial process:
    if (process.env['NODE_ENV'] === 'development') {
      // Wait master to be ready on development mode after each restart.
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    // Create http2 server.
    const server = await createWorkerServer(MASTER_ENDPOINT, WORKER_SECRET, {
      port: PORT,
      host: HOST,
    });

    autoRelease(() => server.close());

    if (process.send) {
      process.send('Ok');
    }

    await waitForExitSignal();
  } finally {
    await releaseAll();
  }
}

main().catch(e => {
  setImmediate(() => {
    throw e;
  });
});
