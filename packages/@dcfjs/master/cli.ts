#!/usr/bin/env node
import {
  autoRelease,
  waitForExitSignal,
  releaseAll,
} from '@dcfjs/common/autoRelease';
import { createMasterServer } from './index';
import { releaseAllClient } from './workerManager';

const PORT = (process.env['PORT'] as any | 0) || 9001;
const HOST = process.env['HOST'] || 'localhost';

async function main() {
  try {
    // Initial process:
    const server = await createMasterServer({
      port: PORT,
      host: HOST,
    });

    // Create http2 server.
    autoRelease(() => server.close());

    autoRelease(releaseAllClient);

    await waitForExitSignal();
  } finally {
    await releaseAll();
  }
}

if (require.main === module) {
  main().catch(e => {
    setImmediate(() => {
      throw e;
    });
  });
}
