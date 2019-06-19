import * as path from 'path';
import * as fs from 'fs';
import { MasterTempStorage } from './tempStorage';

export class FsTempStorage implements MasterTempStorage {
  private _basePath: string;
  constructor(basePath: string) {
    this._basePath = basePath;
  }

  private resolve(key: string) {
    if (/[\.\/\\]/.test(key)) {
      throw new Error('Invalid key.');
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

  deleteItem(key: string) {
    fs.unlinkSync(this.resolve(key));
  }

  cleanUp() {
    if (!fs.existsSync(this._basePath)) {
      fs.mkdirSync(this._basePath);
      return;
    }
    for (const fn of fs.readdirSync(this._basePath)) {
      this.deleteItem(fn);
    }
  }
}
