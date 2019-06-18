import {
  createServer,
  ServerHandlerMap,
  ServerConfig,
} from '@dcfjs/common/server';
import {
  handleRegisterWorker,
  getWorkerStatus,
  dispatchWork,
} from './workerManager';
import { deserializeFunction } from '@dcfjs/common/serializeFunction';
import '@dcfjs/common/registerCaptureEnv';

const ServerHandlers: ServerHandlerMap = {
  '/worker/register': handleRegisterWorker,
  '/worker/status': getWorkerStatus,
  '/exec': func => {
    const f = deserializeFunction(func);
    return f(dispatchWork);
  },
};

export function createMasterServer(config?: ServerConfig) {
  return createServer(ServerHandlers, config);
}
