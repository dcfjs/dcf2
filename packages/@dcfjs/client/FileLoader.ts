export interface FileLoader {
  // run on client.
  // Check if this file loader can handle some uri.
  canHandleUri(baseUri: string): boolean;

  // run on master.
  // list all files in a directory.
  listFiles: (
    baseUri: string,
    recursive: boolean,
  ) => string[] | Promise<string[]>;

  // run on worker
  // load a single file.
  loadFile: (baseUri: string, filename: string) => Buffer | Promise<Buffer>;

  // run on master
  // init save progress, clean old files, create progress marker.
  initSaveProgress: (
    baseUri: string,
    overwrite: boolean,
  ) => void | Promise<void>;

  // run on worker
  // save a single file.
  saveFile: (
    baseUri: string,
    filename: string,
    data: Buffer,
  ) => void | Promise<void>;

  // run on master
  // mark a save work was finished.
  markSaveSuccess: (baseUri: string) => void | Promise<void>;
}
