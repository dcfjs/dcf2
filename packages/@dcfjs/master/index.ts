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
import { ServerHttp2Stream } from 'http2';
import { TempStorage } from '@dcfjs/common/tempStorage';
import { registerTempStorage } from '@dcfjs/common/storageRegistry';

export type ExecTask = (
  dispatchWork: WorkDispatcher,
  createPushStream: (path: string) => Promise<ServerHttp2Stream>,
) => any | Promise<any>;

const ServerHandlers: ServerHandlerMap = {
  '/worker/register': handleRegisterWorker,
  '/worker/status': getWorkerStatus,
  '/exec': async (func, sess, stream) => {
    const f = deserializeFunction(func);
    const createPushStream = (path: string) => pushStream(stream, path);
    return await f(dispatchWork, createPushStream);
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
