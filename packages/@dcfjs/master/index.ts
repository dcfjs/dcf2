import { CleanupFunction } from './../common/autoRelease';
import {
  createServer,
  ServerHandlerMap,
  ServerConfig,
  pushStream,
} from '@dcfjs/common/server';
import {
  handleRegisterWorker,
  getWorkerStatus,
  dispatchWork,
  WorkDispatcher,
} from './workerManager';
import { deserializeFunction } from '@dcfjs/common/serializeFunction';
import '@dcfjs/common/registerCaptureEnv';
import { ServerHttp2Stream, ServerHttp2Session } from 'http2';
import { TempStorage } from '@dcfjs/common/tempStorage';
import { registerTempStorage } from '@dcfjs/common/storageRegistry';

export type ExecTask = (
  dispatchWork: WorkDispatcher,
  createPushStream: (path: string) => Promise<ServerHttp2Stream>,
  autoRelease: (func: CleanupFunction) => void,
) => any | Promise<any>;

type Http2SessionWithAutoRelease = ServerHttp2Session & {
  autoRelease?: (func: CleanupFunction) => void;
};

const ServerHandlers: ServerHandlerMap = {
  '/worker/register': handleRegisterWorker,
  '/worker/status': getWorkerStatus,
  '/exec': async (func, _sess, stream) => {
    const sess = _sess as Http2SessionWithAutoRelease;
    let autoRelease = sess.autoRelease;
    if (!autoRelease) {
      const release: CleanupFunction[] = [];
      autoRelease = sess.autoRelease = func => {
        release.push(func);
      };
      sess.on('close', async () => {
        while (release.length > 0) {
          await release.pop()!();
        }
      });
    }

    const f = deserializeFunction(func);
    const createPushStream = (path: string) => pushStream(stream, path);
    return await f(dispatchWork, createPushStream, autoRelease);
  },
};

export async function createMasterServer(config?: ServerConfig) {
  // Init temp storages.

  if (config && config.storages) {
    // Init temp storages.
    for (const storageConfig of config.storages) {
      let factory = require(storageConfig.module);
      if (factory && factory.default) {
        factory = factory.default;
      }
      const storage: TempStorage = factory(storageConfig.options);
      if (storage.cleanUp) {
        await storage.cleanUp();
      }
      registerTempStorage(storageConfig.name, storage);
    }
  }

  return createServer(ServerHandlers, config);
}
