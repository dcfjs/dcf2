/**
 * @noCaptureEnv
 */
import { createClient, Client } from '@dcfjs/common/client';
import { RDD, GeneratedRDD, PartitionFunc } from './RDD';
import * as http2 from 'http2';
import sf = require('@dcfjs/common/serializeFunction');
import { ExecTask } from '@dcfjs/master';
import split = require('split');
import * as ProgressBar from 'progress';
const v8 = require('v8');

export interface ContextOption {
  showProgress: boolean;
  defaultPartitions: number;
}

const defaultOption: ContextOption = {
  showProgress: !!process.stdout.isTTY,
  defaultPartitions: 4,
};

export class Context {
  private _client: Client;
  private _option: ContextOption;

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
            .on('data', function(line) {
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

  close(): Promise<void> {
    return this._client.close();
  }

  execute<T, T1>(
    numPartitions: number,
    partitionFunc: PartitionFunc<T>,
    finalFunc: (v: T[]) => T1,
  ): Promise<T1> {
    const { showProgress } = this._option;

    return this._client.post<T1>(
      '/exec',
      sf.serializeFunction(
        sf.captureEnv(
          (async (dispatchWork, createPushStream) => {
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

            const partitionResults = await Promise.all(
              new Array(numPartitions)
                .fill(0)
                .map((v, i) => tickAfter(dispatchWork(partitionFunc(i)))),
            );

            if (stream) {
              await new Promise(resolve => stream!.end(resolve));
            }
            return finalFunc(partitionResults);
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
    const partitionCounts = rdds.map(v => v.getNumPartitions());
    const rddFuncs = rdds.map(v => v.getFunc());
    const totalPartitions = partitionCounts.reduce((a, b) => a + b, 0);

    return new GeneratedRDD<T>(
      this,
      totalPartitions,
      sf.captureEnv(
        partitionId => {
          for (let i = 0; i < partitionCounts.length; i++) {
            if (partitionId < partitionCounts[i]) {
              return rddFuncs[i](partitionId);
            }
            partitionId -= partitionCounts[i];
          }
          // `partitionId` should be less than totalPartitions.
          // so it should not reach here.
          throw new Error('Internal error.');
        },
        {
          partitionCounts,
          rddFuncs,
        },
      ),
    );
  }
}

export async function createContext(masterEndpoint: string) {
  return new Context(await createClient(masterEndpoint));
}
