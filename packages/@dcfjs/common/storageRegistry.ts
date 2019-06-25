import { TempStorage } from './tempStorage';

/**
 * Three kind of temporary storage:
 * 1. Shared fs: master & client can access a same volume. Should be cleaned after master stop/restarted.
 * 2. Worker side storage(memory/disk). Should be cleaned after worker stop/restarted.
 * 3. Storage service: gridfs, hdfs, AliYun OSS, etc. Should be cleaned after master stop/restarted.
 */

const registry: {
  [key: string]: TempStorage;
} = {};

export function registerTempStorage(name: string, instance: TempStorage) {
  registry[name] = instance;
}

export function getTempStorage(name: string): TempStorage {
  if (!registry[name]) {
    throw new Error(`Temp storage ${name} does not exists.`);
  }
  return registry[name];
}

export function getStorageEntries() {
  return Object.entries(registry);
}

export function getStorageNames() {
  return Object.keys(registry);
}

export function removeAllStorages() {
  for (const key of Object.keys(registry)) {
    delete registry[key];
  }
}

export class TempStorageSession {
  storages: {
    [type: string]: {
      [key: string]: number;
    };
  } = {};
  timer: NodeJS.Timer;

  constructor() {
    // TODO: move this to config.
    this.timer = setInterval(() => {
      this.refresh();
    }, 1800000);
  }

  addRef(type: string, key: string) {
    getTempStorage(type).refreshExpired(key);
    const map = (this.storages[type] = this.storages[type] || {});
    map[key] = (map[key] || 0) + 1;
  }
  release(type: string, key: string) {
    const map = this.storages[type];
    if (map && map[key]) {
      if (--map[key] <= 0) {
        getTempStorage(type).deleteItem(key);
        delete map[key];
        if (Object.keys(map).length === 0) {
          delete this.storages[type];
        }
      }
    }
  }

  refresh() {
    for (const type of Object.keys(this.storages)) {
      const map = this.storages[type];
      const storage = getTempStorage(type);
      for (const key of Object.keys(map)) {
        storage.refreshExpired(key);
      }
    }
  }

  // close session and refresh timer.
  close() {
    clearInterval(this.timer);

    for (const type of Object.keys(this.storages)) {
      const map = this.storages[type];
      const storage = getTempStorage(type);
      for (const key of Object.keys(map)) {
        storage.deleteItem(key);
      }
    }
  }
}
