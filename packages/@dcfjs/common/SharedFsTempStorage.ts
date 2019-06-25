import '@dcfjs/common/noCaptureEnv';
import * as path from 'path';
import * as fs from 'fs';
import { MasterTempStorage, TempStorage } from './tempStorage';
import { captureEnv } from './serializeFunction';
import { iterator } from './shortId';

/**
 * Shared fs storage:
 * Master & worker should run on same machine.
 */
export class SharedFsTempStorage implements TempStorage {
  protected _basePath: string;
  protected _prefix: string;
  protected _keyItor = iterator();

  constructor(basePath: string, workerId?: string) {
    this._prefix = workerId ? `worker-${workerId}-` : 'master-';
    this._basePath = path.resolve(basePath);
  }

  protected resolve(key: string) {
    if (/[\.\/\\]/.test(key)) {
      throw new Error('Invalid key `' + key + '`');
    }
    return path.resolve(this._basePath, key);
  }

  setItem(key: string, buffer: Buffer) {
    fs.writeFileSync(this.resolve(key), buffer);
  }

  appendItem(key: string, buffer: Buffer) {
    fs.appendFileSync(this.resolve(key), buffer);
  }

  getItem(key: string) {
    return fs.readFileSync(this.resolve(key));
  }

  getAndDeleteItem(key: string) {
    const ret = this.getItem(key);
    this.deleteItem(key);
    return ret;
  }

  deleteItem(key: string) {
    const path = this.resolve(key);
    if (fs.existsSync(path)) {
      fs.unlinkSync(path);
    }
  }

  generateKey() {
    return this._prefix + this._keyItor();
  }

  refreshExpired(key: string) {
    const now = new Date();
    const path = this.resolve(key);
    if (fs.existsSync(path)) {
      fs.utimesSync(path, now, now);
    }
  }
}

export class SharedFsTempMasterStorage extends SharedFsTempStorage
  implements MasterTempStorage {
  // TODO: move this to config.
  expireAfter: number = 3600000;
  constructor(basePath: string) {
    super(basePath);
  }

  cleanUp() {
    const expired = Date.now() - this.expireAfter;

    for (const fn of fs.readdirSync(this._basePath)) {
      const path = this.resolve(fn);
      if (fs.statSync(path).mtimeMs < expired) {
        fs.unlinkSync(path);
      }
    }
  }

  // only cleanup at master side.
  cleanAll() {
    if (!fs.existsSync(this._basePath)) {
      fs.mkdirSync(this._basePath);
      return;
    }
    for (const fn of fs.readdirSync(this._basePath)) {
      fs.unlinkSync(this.resolve(fn));
    }
  }

  getFactory() {
    const basePath = this._basePath;
    return captureEnv(
      ({ workerId }) =>
        new (require('@dcfjs/common/SharedFsTempStorage')).SharedFsTempStorage(
          basePath,
          workerId,
        ),
      {
        basePath,
      },
    );
  }
}

export default function createSharedFsTempMasterStorage({
  basePath = './tmp',
}: {
  basePath?: string;
} = {}) {
  return new SharedFsTempMasterStorage(basePath);
}
