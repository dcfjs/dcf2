import { WorkDispatcher } from '@dcfjs/master/workerManager';
import { createClient, Client } from '@dcfjs/common/client';
import { RDD, GeneratedRDD, PartitionFunc } from './RDD';
// import { captureEnv, serializeFunction } from '@dcfjs/common/serializeFunction';
import sf = require('@dcfjs/common/serializeFunction');

export interface ContextOption {
  defaultPartitions: number;
}

const defaultOption: ContextOption = {
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
  }

  close(): Promise<void> {
    return this._client.close();
  }

  execute<T, T1>(
    numPartitions: number,
    partitionFunc: PartitionFunc<T>,
    finalFunc: (v: T[]) => T1,
  ): Promise<T1> {
    return this._client.post<T1>(
      '/exec',
      sf.serializeFunction(
        sf.captureEnv(
          async (dispatchWork: WorkDispatcher) => {
            const partitionResults = await Promise.all(
              new Array(numPartitions)
                .fill(0)
                .map((v, i) => dispatchWork(partitionFunc(i))),
            );
            return finalFunc(partitionResults);
          },
          {
            numPartitions,
            partitionFunc,
            finalFunc,
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
    const rddFuncs = rdds.map(v => v.getFunc());
    const totalPartition = rddFuncs.map(v => v[0]).reduce((a, b) => a + b, 0);

    return new GeneratedRDD<T>(
      this,
      totalPartition,
      sf.captureEnv(
        partitionId => {
          for (let i = 0; i < rddFuncs.length; i++) {
            if (partitionId < rddFuncs[i][0]) {
              return rddFuncs[i][1](partitionId);
            }
            partitionId -= rddFuncs[i][0];
          }
          // `partitionId` should be less than totalPartition.
          // so it should not reach here.
          throw new Error('Internal error.');
        },
        {
          rddFuncs,
        },
      ),
    );
  }
}

export async function createContext(masterEndpoint: string) {
  return new Context(await createClient(masterEndpoint));
}
