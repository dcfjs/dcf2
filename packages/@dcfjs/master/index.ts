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

export function createMasterServer(config?: ServerConfig) {
  return createServer(ServerHandlers, config);
}
