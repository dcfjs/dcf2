export interface TempStorage {
  setItem(key: string, buffer: Buffer): void | Promise<void>;
  appendItem(key: string, buffer: Buffer): void | Promise<void>;
  getItem(key: string): Buffer | Promise<Buffer>;
  getAndDeleteItem(key: string): Buffer | Promise<Buffer>;
  deleteItem(key: string): void | Promise<void>;
  generateKey(): string | Promise<string>;

  // Remove outdated data(periodly)
  cleanUp?(): void | Promise<void>;

  // Remove all data(after master/worker restarted.)
  cleanAll?(): void | Promise<void>;

  // mark a file that should not be auto cleaned in a period.
  refreshExpired(key: string): void | Promise<void>;
}

export interface MasterTempStorage extends TempStorage {
  // Generate storage at worker
  getFactory(): (options: {
    workerId: string;
    endpoint: string;
  }) => TempStorage;
}
