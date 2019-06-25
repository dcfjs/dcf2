export interface TempStorage {
  setItem(key: string, buffer: Buffer): void | Promise<void>;
  appendItem(key: string, buffer: Buffer): void | Promise<void>;
  getItem(key: string): Buffer | Promise<Buffer>;
  getAndDeleteItem(key: string): Buffer | Promise<Buffer>;
  deleteItem(key: string): void | Promise<void>;

  // Generate a unique key for this storage. It's safe to use it with a suffix starts with `-`.
  generateKey(): string | Promise<string>;

  // Remove outdated data(periodly)
  cleanUp?(): void | Promise<void>;

  // Remove all data(after master/worker restarted.)
  cleanAll?(): void | Promise<void>;

  // mark a file that should not be auto cleaned in a period. if the file doesn't exists, it should ignore and continue.
  refreshExpired(key: string): void | Promise<void>;
}

export interface MasterTempStorage extends TempStorage {
  // Generate storage at worker
  getFactory(): (options: {
    workerId: string;
    endpoint: string;
  }) => TempStorage;
}
