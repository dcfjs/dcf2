import { CleanupFunction, autoRelease } from './../common/autoRelease';
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
import {
  registerTempStorage,
  TempStorageSession,
  removeAllStorages,
  getStorageEntries,
} from '@dcfjs/common/storageRegistry';

export type ExecTask = (
  dispatchWork: WorkDispatcher,
  createPushStream: (path: string) => Promise<ServerHttp2Stream>,
  tempStorageSession: TempStorageSession,
) => any | Promise<any>;

type Http2SessionCustom = ServerHttp2Session & {
  tempStorageSession?: TempStorageSession;
};

const ServerHandlers: ServerHandlerMap = {
  '/worker/register': handleRegisterWorker,
  '/worker/status': getWorkerStatus,
  '/exec': async (func, _sess, stream) => {
    const sess = _sess as Http2SessionCustom;
    if (!sess.tempStorageSession) {
      sess.tempStorageSession = new TempStorageSession();
      sess.on('close', async () => {
        if (sess.tempStorageSession) {
          sess.tempStorageSession.close();
          sess.tempStorageSession = undefined;
        }
      });
    }

    const f = deserializeFunction(func);
    const createPushStream = (path: string) => pushStream(stream, path);
    return await f(dispatchWork, createPushStream, sess.tempStorageSession!);
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
      if (storage.cleanAll) {
        await storage.cleanAll();
      }
      registerTempStorage(storageConfig.name, storage);
    }
  }

  const timer = setInterval(() => {
    for (const [key, storage] of getStorageEntries()) {
      if (storage.cleanUp) {
        storage.cleanUp();
      }
    }
  }, 60000);

  const server = await createServer(ServerHandlers, config);
  server.on('close', () => {
    clearInterval(timer);
    removeAllStorages();
  });
  return server;
}
