import { WorkDispatcher } from '@dcfjs/master/workerManager';
import { createClient, Client } from '@dcfjs/common/client';
import { RDD, GeneratedRDD, PartitionFunc } from './RDD';
import { captureEnv, serializeFunction } from '@dcfjs/common/serializeFunction';

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
      serializeFunction(
        captureEnv(
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

    interface Arg {
      from: number;
      count: number;
    }

    const args: Arg[] = [];
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
      captureEnv(
        partitionId => {
          const { from, count } = args[partitionId];
          // TODO: This is ugly.
          return (global as any).__captureEnv(
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
        { step, args },
      ),
    );
  }
}

export async function createContext(masterEndpoint: string) {
  return new Context(await createClient(masterEndpoint));
}
