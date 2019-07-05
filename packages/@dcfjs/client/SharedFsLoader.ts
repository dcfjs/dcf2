import '@dcfjs/common/noCaptureEnv';
import { FileLoader } from './FileLoader';
import sf = require('@dcfjs/common/serializeFunction');
import fs = require('fs');
import path = require('path');
import os = require('os');
import url = require('url');

const walkSync = sf.captureEnv(
  function(dirPath: string, filelist: string[], recursive = false) {
    function work(dirPath: string) {
      const files = fs.readdirSync(dirPath);
      files.forEach(function(file: string) {
        if (recursive) {
          if (fs.statSync(dirPath + file).isDirectory()) {
            work(dirPath + file + '/');
          } else {
            filelist.push(dirPath + file);
          }
        } else {
          if (!fs.statSync(dirPath + file).isDirectory()) {
            filelist.push(dirPath + file);
          }
        }
      });
    }
    work(dirPath);
  },
  {
    fs: sf.requireModule('fs'),
  },
);

// remove all content in a directory except itself.
const rmdirs = sf.captureEnv(
  function(basePath: string) {
    function work(basePath: string) {
      const stat = fs.lstatSync(basePath);
      if (stat.isDirectory()) {
        const contents = fs.readdirSync(basePath);
        for (const file of contents) {
          work(path.resolve(basePath, file));
        }
        fs.rmdirSync(basePath);
      } else {
        fs.unlinkSync(basePath);
      }
    }
    const contents = fs.readdirSync(basePath);
    for (const file of contents) {
      work(path.resolve(basePath, file));
    }
  },
  {
    path: sf.requireModule('path'),
    fs: sf.requireModule('fs'),
  },
);

const solvePath = sf.captureEnv(
  (baseUri: string, ...filenames: string[]): string => {
    const uri = url.parse(baseUri);
    if (uri.protocol) {
      // absolute path
      const ret = uri.path!;
      if (os.platform() === 'win32') {
        return path.resolve(ret.substr(1), ...filenames);
      }
      return path.resolve(ret, ...filenames);
    } else {
      // maybe relative
      return path.resolve(process.cwd(), baseUri, ...filenames);
    }
  },
  {
    os: sf.requireModule('os'),
    path: sf.requireModule('path'),
    url: sf.requireModule('url'),
  },
);

export class SharedFsLoader implements FileLoader {
  canHandleUri(baseUri: string): boolean {
    const uri = url.parse(baseUri);
    if (uri.protocol && uri.protocol !== 'file:') {
      return false;
    }
    return true;
  }

  listFiles: (
    baseUri: string,
    recursive: boolean,
  ) => string[] | Promise<string[]> = sf.captureEnv(
    (baseUri: string, recursive: boolean) => {
      const fileList = [] as string[];
      walkSync(baseUri, fileList, recursive);
      return fileList;
    },
    {
      walkSync,
    },
  );

  loadFile: (
    baseUri: string,
    filename: string,
  ) => Buffer | Promise<Buffer> = sf.captureEnv(
    (baseUri: string, filename: string) => {
      const fn = solvePath(baseUri, filename);
      return fs.readFileSync(fn);
    },
    {
      fs: sf.requireModule('fs'),
      path: sf.requireModule('path'),
      solvePath,
    },
  );

  initSaveProgress: (
    baseUri: string,
    overwrite: boolean,
  ) => void | Promise<void> = sf.captureEnv(
    (baseUri: string, overwrite: boolean) => {
      const basePath = solvePath(baseUri);

      if (!fs.existsSync(basePath)) {
        fs.mkdirSync(basePath);
      } else {
        if (!overwrite) {
          throw new Error(
            `${basePath} already exists, consider use overwrite=true?`,
          );
        }
        rmdirs(basePath);
      }

      // Create a writing mark.
      fs.writeFileSync(path.resolve(basePath, '.writing'), Buffer.alloc(0));
    },
    {
      fs: sf.requireModule('fs'),
      path: sf.requireModule('path'),
      solvePath,
    },
  );

  saveFile: (
    baseUri: string,
    filename: string,
    data: Buffer,
  ) => void | Promise<void> = sf.captureEnv(
    (baseUri: string, filename: string, data: Buffer) => {
      const path = solvePath(baseUri, filename);
      fs.writeFileSync(path, data);
    },
    {
      fs: sf.requireModule('fs'),
      solvePath,
    },
  );
  markSaveSuccess: (baseUri: string) => void | Promise<void> = sf.captureEnv(
    (baseUri: string) => {
      const basePath = solvePath(baseUri);
      fs.renameSync(
        path.resolve(basePath, '.writing'),
        path.resolve(basePath, '.success'),
      );
    },
    {
      fs: sf.requireModule('fs'),
      path: sf.requireModule('path'),
      solvePath,
    },
  );
}
