import { FileLoader } from './FileLoader';
import '@dcfjs/common/noCaptureEnv';
import { TempStorageSession } from './../common/storageRegistry';
import { GeneratedRDD, FileLoaderRDD, RDD, UnionRDD } from './RDD';
import { Client, createClient } from '@dcfjs/common/client';
import * as http2 from 'http2';
import sf = require('@dcfjs/common/serializeFunction');
import { ExecTask } from '@dcfjs/master';
import * as ProgressBar from 'progress';
import { FunctionEnv, SerializedFunction } from '@dcfjs/common';
import split = require('split');

const v8 = require('v8');

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

export class Context {
  private _client: Client;
  private _option: ContextOption;
  private _fileLoaders: FileLoader[] = [];

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

  registerFileLoader(loader: FileLoader) {
    this._fileLoaders.push(loader);
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
              sendProgress = () => {};
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

  getFileLoader(baseUri: string) {
    return this._fileLoaders.find(v => v.canHandleUri(baseUri));
  }

  binaryFiles(
    baseUri: string,
    {
      recursive = false,
    }: {
      recursive?: boolean;
    } = {},
  ): RDD<[string, Buffer]> {
    const loader = this.getFileLoader(baseUri);

    if (!loader) {
      throw new Error('No loader can handle uri: `' + baseUri + '`');
    }
    const { listFiles, loadFile } = loader;

    return new FileLoaderRDD(
      this,
      sf.captureEnv(
        () => {
          return listFiles(baseUri, recursive);
        },
        {
          listFiles,
          baseUri,
          recursive,
        },
      ),
      sf.captureEnv(
        (fileName: string) => {
          return loadFile(baseUri, fileName);
        },
        {
          loadFile,
          baseUri,
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
  return context;
}
