import '@dcfjs/common/noCaptureEnv';
import { TempStorageSession } from './../common/storageRegistry';
import { GeneratedRDD, LoadedFileRDD, RDD, UnionRDD } from './RDD';
import { Client, createClient } from '@dcfjs/common/client';
import * as http2 from 'http2';
import sf = require('@dcfjs/common/serializeFunction');
import { ExecTask } from '@dcfjs/master';
import * as ProgressBar from 'progress';
import { FunctionEnv, SerializedFunction } from '@dcfjs/common';
import split = require('split');

const v8 = require('v8');
const fs = require('fs');
const urlLib = require('url');
const oss = require('ali-oss');

export interface ContextOption {
  showProgress: boolean;
  defaultPartitions: number;
  defaultPersistStorage: string;
  defaultRepartitionStorage: string;
}

const defaultOption: ContextOption = {
  showProgress: !!process.stdout.isTTY,
  defaultPartitions: 4,
  defaultPersistStorage: 'disk',
  defaultRepartitionStorage: 'disk',
};

const walkSync = sf.captureEnv(
  function(dirPath: string, filelist: string[], recursive = false) {
    const files = fs.readdirSync(dirPath);
    files.forEach(function(file: string) {
      if (recursive) {
        if (fs.statSync(dirPath + file).isDirectory()) {
          walkSync(dirPath + file + '/', filelist, recursive);
        } else {
          filelist.push(dirPath + file);
        }
      } else {
        if (!fs.statSync(dirPath + file).isDirectory()) {
          filelist.push(dirPath + file);
        }
      }
    });
  },
  {
    fs: sf.requireModule('fs'),
  },
);

const recursiveRemoveSync = sf.captureEnv(
  function(dirPath: string) {
    if (!dirPath.endsWith('/')) {
      dirPath = dirPath + '/';
    }

    const files = fs.readdirSync(dirPath);
    files.forEach(function(file: string) {
      if (fs.statSync(dirPath + file).isDirectory()) {
        recursiveRemoveSync(dirPath + file + '/');
        fs.rmdirSync(dirPath + file + '/');
      } else {
        fs.unlinkSync(dirPath + file);
      }
    });
  },
  {
    fs: sf.requireModule('fs'),
  },
);

const parseOSSUrl = sf.captureEnv(
  function(baseUrl: string) {
    const url = urlLib.parse(baseUrl);
    return [url.host, url.path.substr(1)];
  },
  {
    urlLib: sf.requireModule('url'),
  },
);

function localFsLoader(): { [key: string]: (...args: any[]) => any } {
  function canHandleUrl(baseUrl: string) {
    return baseUrl[0] === '/';
  }

  function listFiles(baseUrl: string, recursive = false) {
    const fileList = [] as string[];
    walkSync(baseUrl, fileList, recursive);
    return fileList;
  }

  function createDataLoader(baseUrl: string) {
    if (!baseUrl.endsWith('/')) {
      baseUrl = baseUrl + '/';
    }

    function loader(filename: string) {
      return fs.readFileSync(filename);
    }

    return sf.captureEnv(loader, {
      fs: sf.requireModule('fs'),
    });
  }

  function initSaveProgress(baseUrl: string, overwrite: boolean) {
    if (!fs.statSync(baseUrl).isDirectory()) {
      throw new Error(`${baseUrl} is not a folder.`);
    }

    if (!baseUrl.endsWith('/')) {
      baseUrl = baseUrl + '/';
    }

    if (overwrite) {
      // remove all files under baseUrl;
      recursiveRemoveSync(baseUrl);
    } else {
      const fileList = fs.readdirSync(baseUrl);
      if (fileList.length > 0) {
        throw new Error(
          `${baseUrl} is not empty, consider use overwrite=true?`,
        );
      }
    }

    fs.writeFileSync(baseUrl + '.writing', Buffer.alloc(0));
  }

  function createDataSaver(baseUrl: string) {
    function loader(filename: string, buffer: Buffer) {
      return fs.writeFileSync(filename, buffer);
    }

    return sf.captureEnv(loader, {
      fs: sf.requireModule('fs'),
    });
  }

  function markSaveSuccess(baseUrl: string) {
    fs.writeFileSync(baseUrl + '.success', Buffer.alloc(0));
    fs.unlinkSync(baseUrl + '.writing');
  }

  return {
    canHandleUrl: canHandleUrl,
    listFiles: sf.captureEnv(listFiles, {
      walkSync,
      sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      fs: sf.requireModule('fs'),
    }),
    createDataLoader: sf.captureEnv(createDataLoader, {
      sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      fs: sf.requireModule('fs'),
    }),
    initSaveProgress: sf.captureEnv(initSaveProgress, {
      recursiveRemoveSync,
      sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      fs: sf.requireModule('fs'),
    }),
    createDataSaver: sf.captureEnv(createDataSaver, {
      sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      fs: sf.requireModule('fs'),
    }),
    markSaveSuccess: sf.captureEnv(markSaveSuccess, {
      sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      fs: sf.requireModule('fs'),
    }),
  };
}

function aliOSSLoader(config: {
  accessKeyId: string;
  accessKeySecret: string;
  bucket: string;
  region: string;
  internal: boolean;
}) {
  function canHandleUrl(baseUrl: string) {
    return baseUrl.indexOf(`oss://${config.bucket}/`) === 0;
  }

  async function listFiles(baseUrl: string, recursive = false) {
    const store = oss(config);
    const [bucket, prefix] = parseOSSUrl(baseUrl);
    const ret = [];

    let marker = null;

    for (;;) {
      let nextMarker: any, objects: any;
      ({ nextMarker, objects } = await store.list({
        prefix: prefix,
        delimiter: recursive ? null : '/',
        marker,
      }));
      if (objects) {
        ret.push(objects.map((v: any) => v.name));
      }
      if (nextMarker === null) {
        break;
      }
      marker = nextMarker;
    }

    return []
      .concat(...ret)
      .map((v: string) => v.substr(prefix.length))
      .filter(v => !v.startsWith('.'));
  }

  function createDataLoader(baseUrl: string) {
    let store = [] as any[];
    const [bucket, prefix] = parseOSSUrl(baseUrl);

    async function loader(filename: string) {
      if (!store[0]) {
        store[0] = oss(config).useBucket(bucket);
      }
      const finalName = prefix + filename;
      const info = await store[0].head(finalName);
      const fileSize = +info.res.headers['content-length'];
      if (fileSize > 1 << 16) {
        // download with multi thread.
        const threadCount = 8;
        const range = [];
        for (let i = 0; i < threadCount; i++) {
          range.push(Math.floor((fileSize / threadCount) * i));
        }
        range.push(fileSize);
        const works = [];
        for (let i = 0; i < threadCount; i++) {
          if (!store[i]) {
            store[i] = oss(config).useBucket(bucket);
          }
          works.push(
            store[i].get(finalName, {
              headers: { Range: `bytes=${range[i]}-${range[i + 1] - 1}` },
            }),
          );
        }
        const resps = (await Promise.all(works)).map(v => v.content);
        return Buffer.concat(resps);
      }
      return store[0].get(finalName).then((v: any) => v.content);
    }

    return sf.captureEnv(loader, {
      config,
      store,
      bucket,
      prefix,
      oss: sf.requireModule('ali-oss'),
    });
  }

  async function initSaveProgress(baseUrl: string, overwrite = false) {
    const store = oss(config);
    const [bucket, prefix] = parseOSSUrl(baseUrl);
    if (overwrite) {
      // delete every file with same prefix. Dangerous!
      for (;;) {
        const { objects } = await store.list({
          prefix: prefix,
          'max-keys': 1,
        });

        if (!objects || !objects.length) {
          break;
        }
        await store.deleteMulti(objects.map((v: any) => v.name));
      }
    } else {
      // Check there's any file with same prefix.
      const { objects } = await store.list({
        prefix: prefix,
        'max-keys': 1,
      });
      if (objects && objects.length) {
        throw new Error(
          `${baseUrl} already exists, consider use overwrite=true?`,
        );
      }
    }
    await store.put(prefix + '.writing', Buffer.alloc(0));
  }

  function createDataSaver(baseUrl: string) {
    let store = null as any;
    const [bucket, prefix] = parseOSSUrl(baseUrl);

    function loader(filename: string, buffer: Buffer) {
      if (!store) {
        store = oss(config);
        store.useBucket(bucket);
      }
      return store.put(prefix + filename, buffer);
    }

    return sf.captureEnv(loader, {
      config,
      store,
      bucket,
      prefix,
      oss: sf.requireModule('ali-oss'),
    });
  }

  async function markSaveSuccess(baseUrl: string) {
    const store = oss(config);
    const [bucket, prefix] = parseOSSUrl(baseUrl);
    await store.put(prefix + '.success', Buffer.alloc(0));
    await store.delete(prefix + '.writing');
  }

  return {
    canHandleUrl: sf.captureEnv(canHandleUrl, {
      config,
      urlLib: sf.requireModule('url'),
    }),
    listFiles: sf.captureEnv(listFiles, {
      config,
      parseOSSUrl,
      sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      urlLib: sf.requireModule('url'),
      oss: sf.requireModule('ali-oss'),
    }),
    createDataLoader: sf.captureEnv(createDataLoader, {
      config,
      parseOSSUrl,
      sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      urlLib: sf.requireModule('url'),
      oss: sf.requireModule('ali-oss'),
    }),
    initSaveProgress: sf.captureEnv(initSaveProgress, {
      config,
      parseOSSUrl,
      sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      urlLib: sf.requireModule('url'),
      oss: sf.requireModule('ali-oss'),
    }),
    createDataSaver: sf.captureEnv(createDataSaver, {
      config,
      parseOSSUrl,
      sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      urlLib: sf.requireModule('url'),
      oss: sf.requireModule('ali-oss'),
    }),
    markSaveSuccess: sf.captureEnv(markSaveSuccess, {
      config,
      parseOSSUrl,
      sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      urlLib: sf.requireModule('url'),
      oss: sf.requireModule('ali-oss'),
    }),
  };
}

export class Context {
  private _client: Client;
  private _option: ContextOption;
  private _fileLoaders: { [key: string]: SerializedFunction<any> }[] = [];

  constructor(client: Client, option?: Partial<ContextOption>) {
    this._client = client;
    this._option = {
      ...defaultOption,
      ...option,
    };

    this._client.session.on('stream', (stream, header) => {
      const path = header[http2.constants.HTTP2_HEADER_PATH];
      switch (path) {
        case '/progress': {
          let progress: ProgressBar | null;

          stream
            .pipe(split())
            .on('data', function(line: string) {
              if (!line) {
                return;
              }
              const [curr, total] = v8.deserialize(Buffer.from(line, 'base64'));
              if (!progress) {
                progress = new ProgressBar(
                  `:percent [:bar] Partitions :current/:total :rate/s :etas`,
                  {
                    clear: true,
                    curr,
                    total,
                  },
                );
              } else {
                progress.tick();
              }
            })
            .on('end', () => {
              if (progress) {
                progress.terminate();
                progress = null;
              }
            });
          break;
        }
        default: {
          stream.close();
        }
      }
    });
  }

  get client() {
    return this._client;
  }

  get option() {
    return this._option;
  }

  registerFileLoader(loaderFuncs: { [key: string]: (...args: any[]) => any }) {
    const validFuncNames = [
      'canHandleUrl',
      'listFiles',
      'createDataLoader',
      'initSaveProgress',
      'createDataSaver',
      'markSaveSuccess',
    ];

    const registerObj = {} as { [key: string]: SerializedFunction<any> };

    for (const funcName of validFuncNames) {
      if (!loaderFuncs[funcName]) {
        throw new Error(`Loader function ${funcName} not implemented.`);
      }

      registerObj[funcName] = sf.serializeFunction(loaderFuncs[funcName]);
    }

    this._fileLoaders.push(registerObj);
  }

  close(): Promise<void> {
    return this._client.close();
  }

  async execute<T, T1, T2 = any>(
    numPartitions: number,
    partitionFunc: (
      paritionId: number,
      tempStorageSession: TempStorageSession,
    ) =>
      | ((workerId: string) => T | Promise<T>) // single function runs on worker.
      | ([(workerId: string) => T2 | Promise<T2>, (arg: T2) => T | Promise<T>]), // first function run on worker, second function run on master
    finalFunc: (
      v: T[],
      tempStorageSession: TempStorageSession,
    ) => T1 | Promise<T1>,
  ): Promise<T1> {
    const { showProgress } = this._option;

    const ret = await this._client.post<T1>(
      '/exec',
      sf.serializeFunction(
        sf.captureEnv(
          (async (dispatchWork, createPushStream, tempStorageSession) => {
            let sendProgress: (current: number, total: number) => void;
            let stream: http2.ServerHttp2Stream | undefined;
            if (showProgress) {
              stream = await createPushStream('/progress');
              sendProgress = (current, total) => {
                stream!.write(
                  (v8.serialize([current, total]) as Buffer).toString(
                    'base64',
                  ) + '\n',
                );
              };
            } else {
              sendProgress = () => { };
            }

            let current = 0;
            sendProgress(0, numPartitions);
            function tickAfter<T>(p: Promise<T>): Promise<T> {
              p.then(() => {
                sendProgress(++current, numPartitions);
              });
              return p;
            }

            try {
              const partitionResults = await Promise.all(
                new Array(numPartitions).fill(0).map((v, i) => {
                  const f = partitionFunc(i, tempStorageSession);
                  if (Array.isArray(f)) {
                    return tickAfter(dispatchWork(f[0]).then(f[1]));
                  }
                  return tickAfter(dispatchWork(f));
                }),
              );

              return await finalFunc(partitionResults, tempStorageSession);
            } finally {
              if (stream) {
                await new Promise(resolve => stream!.end(resolve));
              }
            }
          }) as ExecTask,
          {
            numPartitions,
            partitionFunc,
            finalFunc,
            showProgress,
            v8: sf.requireModule('v8'),
          },
        ),
      ),
    );
    return ret;
  }

  emptyRDD(): RDD<never> {
    return new GeneratedRDD<never>(this, 0);
  }

  range(to: number): RDD<number>;
  range(
    from: number,
    to?: number,
    step?: number,
    numPartitions?: number,
  ): RDD<number>;
  range(
    from: number,
    to?: number,
    step: number = 1,
    numPartitions?: number,
  ): RDD<number> {
    if (to == null) {
      to = from;
      from = 0;
    }
    numPartitions = numPartitions || this._option.defaultPartitions;
    const finalCount = Math.ceil((to - from) / step);

    const rest = finalCount % numPartitions;
    const eachCount = (finalCount - rest) / numPartitions;

    const args: Array<{ from: number; count: number }> = [];
    let index = 0;
    for (let i = 0; i < numPartitions; i++) {
      const subCount = i < rest ? eachCount + 1 : eachCount;
      const end = index + subCount;
      args.push({
        from: from + step * index,
        count: subCount,
      });
      index = end;
    }

    return new GeneratedRDD<number>(
      this,
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const { from, count } = args[partitionId];
          return sf.captureEnv(
            () => {
              const ret = [];
              for (let i = 0; i < count; i++) {
                ret.push(from + step * i);
              }
              return ret;
            },
            { from, count, step },
          );
        },
        { step, args, sf: sf.requireModule('@dcfjs/common/serializeFunction') },
      ),
    );
  }

  parallelize<T>(arr: T[], numPartitions?: number): RDD<T> {
    numPartitions = numPartitions || this._option.defaultPartitions;
    const args: T[][] = [];

    const rest = arr.length % numPartitions;
    const eachCount = (arr.length - rest) / numPartitions;

    let index = 0;
    for (let i = 0; i < numPartitions; i++) {
      const subCount = i < rest ? eachCount + 1 : eachCount;
      const end = index + subCount;
      args.push(arr.slice(index, end));
      index = end;
    }

    return new GeneratedRDD<T>(
      this,
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const data = args[partitionId];
          return sf.captureEnv(() => data, {
            data,
          });
        },
        {
          args,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
    );
  }

  union<T>(...rdds: RDD<T>[]): RDD<T> {
    return new UnionRDD<T>(this, rdds);
  }

  binaryFiles(
    baseUrl: string,
    {
      recursive = false,
    }: {
      recursive?: boolean;
    } = {},
  ): RDD<[string, Buffer]> {
    let selectedLoader: any;

    for (const loader of this._fileLoaders) {
      const canHandleUrlFunc = sf.deserializeFunction(loader.canHandleUrl);

      if (canHandleUrlFunc(baseUrl)) {
        selectedLoader = loader;
        break;
      }
    }

    if (!selectedLoader) {
      throw new Error('No loader found for ' + baseUrl);
    }

    return new LoadedFileRDD<[string, Buffer]>(
      this,
      sf.captureEnv(
        partitionId =>
          sf.captureEnv(
            () => {
              const deserialized = {} as {
                [key: string]: (...args: any[]) => any;
              };

              for (const key of Object.keys(selectedLoader)) {
                deserialized[key] = sf.deserializeFunction(selectedLoader[key]);
              }

              return Promise.resolve(
                deserialized.listFiles(baseUrl, recursive),
              ).then(fileList => {
                const fileName = fileList[partitionId];

                const loader = deserialized.createDataLoader(baseUrl);
                return Promise.resolve(loader(fileName)).then(fileContent => {
                  return [[fileName, fileContent]];
                });
              });
            },
            {
              partitionId,
              baseUrl,
              recursive,
              selectedLoader,
              sf: sf.requireModule('@dcfjs/common/serializeFunction'),
            },
          ),
        {
          baseUrl,
          recursive,
          selectedLoader,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
      sf.captureEnv(
        () => {
          const fileListFunc = sf.deserializeFunction(selectedLoader.listFiles);
          return Promise.resolve(fileListFunc(baseUrl, recursive));
        },
        {
          baseUrl,
          recursive,
          selectedLoader: selectedLoader,
        },
      ),
    );
  }

  wholeTextFiles(
    baseUrl: string,
    {
      decompressor,
      encoding = 'utf8',
      recursive = false,
      functionEnv,
    }: {
      encoding?: string;
      recursive?: boolean;

      decompressor?: (data: Buffer, filename: string) => Buffer;
      functionEnv?: FunctionEnv;
    } = {},
  ): RDD<[string, string]> {
    if (typeof encoding === 'boolean') {
      recursive = encoding;
      encoding = 'utf-8';
    }
    if (typeof decompressor === 'function') {
      sf.captureEnv(decompressor, functionEnv!);
    }
    return this.binaryFiles(baseUrl, { recursive }).mapPartitions(
      v => {
        let buf = v[0][1];
        if (decompressor) {
          buf = decompressor(buf, v[0][0]);
        }
        return [[v[0][0], buf.toString(encoding)] as [string, string]];
      },
      { encoding, decompressor },
    );
  }

  textFile(
    baseUrl: string,
    options?: {
      encoding?: string;
      recursive?: boolean;

      decompressor?: (data: Buffer, filename: string) => Buffer;
      functionEnv?: FunctionEnv;

      __dangerousDontCopy?: boolean;
    },
  ): RDD<string> {
    const { __dangerousDontCopy: dontCopy = false } = options || {};

    return this.wholeTextFiles(baseUrl, options).flatMap(
      (v: any) => {
        const ret = v[1].replace(/\\r/m, '').split('\n');
        // Remove last empty line.
        if (!ret[ret.length - 1]) {
          ret.pop();
        }
        if (dontCopy) {
          return ret;
        }
        // Fix memory leak: sliced string keep reference of huge string
        // see https://bugs.chromium.org/p/v8/issues/detail?id=2869
        return ret.map((v: any) => (' ' + v).substr(1));
      },
      {
        dontCopy,
      },
    );
  }
}

export async function createContext(masterEndpoint: string) {
  const context = new Context(await createClient(masterEndpoint));
  context.registerFileLoader(localFsLoader());
  return context;
}
