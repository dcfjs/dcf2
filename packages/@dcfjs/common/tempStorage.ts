export interface TempStorage {
  setItem(key: string, buffer: Buffer): void | Promise<void>;
  appendItem(key: string, buffer: Buffer): void | Promise<void>;
  getItem(key: string): Buffer | Promise<Buffer>;
  deleteItem(key: string): void | Promise<void>;
}

export interface MasterTempStorage extends TempStorage {
  // Clean up whole storage after master restarted.
  cleanUp(): void | Promise<void>;
}
