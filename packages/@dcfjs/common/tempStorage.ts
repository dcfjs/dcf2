export interface TempStorage {
  setItem(key: string, buffer: Buffer): void | Promise<void>;
  appendItem(key: string, buffer: Buffer): void | Promise<void>;
  getItem(key: string): Buffer | Promise<Buffer>;
  deleteItem(key: string): void | Promise<void>;
  generateKey(): string | Promise<void>;

  // Clean up storage at worker side.
  cleanUp?(): void | Promise<void>;
}

export interface MasterTempStorage extends TempStorage {
  // Clean up storage at master side.
  cleanUp?(): void | Promise<void>;

  // Generate storage at worker
  getFactory(): (options: {
    workerId: string;
    endpoint: string;
  }) => TempStorage;
}
