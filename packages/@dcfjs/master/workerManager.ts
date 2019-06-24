import { ServerBadRequestError } from '@dcfjs/common/server';
import { Client, createClient } from '@dcfjs/common/client';
import { iterator } from '@dcfjs/common/shortId';
import {
  SerializedFunction,
  FunctionEnv,
  serializeFunction,
} from '@dcfjs/common/serializeFunction';

import debugFactory from 'debug';

const debug = debugFactory('master:worker');

const SECRET = process.env['WORKER_SECRET'] || 'NO_SECRET';

const idGen = iterator();

// list of all workers.
const workerList: ClientWorker[] = [];

// list of idle workers.
const idleList: ClientWorker[] = [];

// list of pending works.
type Work<T> = [
  SerializedFunction<() => T>,
  (result: T) => void,
  (reason: any) => void
];
const pendingList: Work<any>[] = [];

class ClientWorker {
  private id: string;
  private client: Client;
  private workerListPos: number | null = null;
  private idleListPos: number | null = null;

  constructor(id: string, client: Client) {
    this.id = id;
    this.client = client;
    client.session.on('close', () => {
      this.handleClose();
      // delete workers[id];
      debug(`Worker ${id} disconnected.`);
    });
    this._addToWorkerList();
    this._becomeIdle();
  }

  private _addToWorkerList() {
    this.workerListPos = workerList.length;
    workerList.push(this);
  }
  private _removeFromWorkerList() {
    if (this.workerListPos != null) {
      if (this.workerListPos === workerList.length - 1) {
        workerList.pop();
      } else {
        const tmp = workerList.pop()!;
        workerList[this.workerListPos] = tmp;
        tmp.workerListPos = this.workerListPos;
      }
      this.workerListPos = null;
      if (workerList.length === 0) {
        // Cancel all pending works, to avoid infinite waiting.
        let work;
        while ((work = pendingList.pop())) {
          const [func, resolve, reject] = work;
          reject(new Error('No workers available.'));
        }
      }
    }
  }
  private _becomeIdle() {
    if (pendingList.length) {
      const [func, resolve, reject] = pendingList.shift()!;
      this.handleWork<any>(func).then(resolve, reject);
    } else {
      this._addToIdleList();
    }
  }
  private _addToIdleList() {
    this.idleListPos = idleList.length;
    idleList.push(this);
  }
  private _removeFromIdleList() {
    if (this.idleListPos != null) {
      if (this.idleListPos === idleList.length - 1) {
        idleList.pop();
      } else {
        const tmp = idleList.pop()!;
        idleList[this.idleListPos] = tmp;
        tmp.idleListPos = this.idleListPos;
      }
      this.workerListPos = null;
    }
  }

  handleClose() {
    this._removeFromWorkerList();
    this._removeFromIdleList();
  }

  handleWork<T>(func: SerializedFunction<() => T>): Promise<T> {
    const ret = this.client.post<T>('/exec', func);
    ret.finally(() => this._becomeIdle());
    ret.catch(e => {
      console.warn(JSON.stringify(func, undefined, 2));
    });
    return ret;
  }

  static dispatchWork<T>(
    func: SerializedFunction<() => T>,
    resolve: (result: T) => void,
    reject: (reason: any) => void,
  ) {
    if (idleList.length > 0) {
      const worker = idleList.pop()!;
      worker.idleListPos = null;
      worker.handleWork<T>(func).then(resolve, reject);
    } else if (workerList.length === 0) {
      reject(new Error('No workers available.'));
    } else {
      pendingList.push([func, resolve, reject]);
    }
  }

  close(): Promise<void> {
    return this.client.close();
  }
}

export const handleRegisterWorker = async ({
  endpoint,
  secret,
}: {
  endpoint: string;
  secret: string;
}) => {
  if (secret !== SECRET) {
    throw new ServerBadRequestError('Bad Secret');
  }
  try {
    const client = await createClient(endpoint);
    const id = idGen();
    await client.post('/init', { secret: SECRET, id });
    new ClientWorker(id, client);
    debug(`Worker ${id} connected from ${endpoint}.`);
  } catch (e) {
    throw new Error('Failed to contact with worker:\n\t' + e.message);
  }
};

export function getWorkerStatus() {
  return [workerList.length, idleList.length, pendingList.length];
}

export function releaseAllClient() {
  const promises = [];
  for (const worker of workerList) {
    promises.push(worker.close());
  }
  return Promise.all(promises);
}

export function dispatchWork<T = any>(
  func: SerializedFunction<() => T | Promise<T>> | (() => T | Promise<T>),
  env?: FunctionEnv,
): Promise<T> {
  if (typeof func === 'function') {
    func = serializeFunction(func, env);
  }
  return new Promise<T>((resolve, reject) => {
    ClientWorker.dispatchWork(
      func as SerializedFunction<() => T>,
      resolve,
      reject,
    );
  });
}

export type WorkDispatcher = typeof dispatchWork;
