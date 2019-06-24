import { TempStorage } from './tempStorage';

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
