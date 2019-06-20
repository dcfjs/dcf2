import { WorkDispatcher } from '@dcfjs/master/workerManager';
import { Context } from './Context';
import { concatArrays, defaultComparator } from '@dcfjs/common/arrayHelpers';
import sf = require('@dcfjs/common/serializeFunction');

export type ParallelTask = (dispachWorker: WorkDispatcher) => any;
export type PartitionFunc<T> = (paritionId: number) => () => T | Promise<T>;

export class RDD<T> {
  protected _context: Context;
  protected constructor(context: Context) {
    this._context = context;
  }
  getFunc(): PartitionFunc<T[]> {
    throw new Error('Must be overrided.');
  }

  getNumPartitions(): number {
    throw new Error('Must be overrided.');
  }

  union(...others: RDD<T>[]): RDD<T> {
    return this._context.union(this, ...others);
  }

  collect(): Promise<T[]> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = this.getFunc();

    return this._context.execute(numPartitions, partitionFunc, concatArrays);
  }

  count(): Promise<number> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = this.getFunc();

    return this._context.execute(
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          return sf.captureEnv(
            () => {
              return Promise.resolve(f()).then(v => v.length);
            },
            { f },
          ) as () => Promise<number>;
        },
        {
          partitionFunc,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
      v => v.reduce((a, b) => a + b, 0),
    );
  }

  take(count: number): Promise<T[]> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = this.getFunc();

    return this._context.execute(
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          return sf.captureEnv(
            () => {
              return Promise.resolve(f()).then((v: T[]) => v.slice(0, count));
            },
            { f },
          );
        },
        {
          count,
          partitionFunc,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
      v => concatArrays(v).slice(0, count),
    );
  }

  max(comparator = defaultComparator): Promise<T | null> {
    return this.reduce(
      sf.captureEnv(
        (a: any, b: any) => {
          return comparator(a, b) > 0 ? a : b;
        },
        {
          comparator,
        },
      ),
    );
  }

  min(comparator = defaultComparator): Promise<T | null> {
    return this.reduce(
      sf.captureEnv(
        (a: any, b: any) => {
          return comparator(a, b) < 0 ? a : b;
        },
        {
          comparator,
        },
      ),
    );
  }

  mapPartition<T1>(transformer: (input: T[]) => T1[]): RDD<T1> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = this.getFunc();

    return new GeneratedRDD<T1>(
      this._context,
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          return sf.captureEnv(
            () => {
              return Promise.resolve(f()).then((v: T[]) => transformer(v));
            },
            { f, transformer },
          );
        },
        {
          transformer,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
    );
  }

  glom(): RDD<T[]> {
    return this.mapPartition(v => [v]);
  }

  map<T1>(transformer: (input: T) => T1): RDD<T1> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = this.getFunc();

    return new GeneratedRDD<T1>(
      this._context,
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          return sf.captureEnv(
            () => {
              return Promise.resolve(f()).then(arr => {
                const resArr = new Array(arr.length);
                for (let i = 0; i < arr.length; i++) {
                  resArr[i] = transformer(arr[i]);
                }
                return resArr;
              });
            },
            { f, transformer },
          );
        },
        {
          transformer,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
    );
  }

  flatMap<T1>(transformer: (input: T) => T1[]): RDD<T1> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = this.getFunc();

    return new GeneratedRDD<T1>(
      this._context,
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          return sf.captureEnv(
            () => {
              return Promise.resolve(f()).then(arr => {
                const newArr = [];
                for (let i = 0; i < arr.length; i++) {
                  const tmpRes = transformer(arr[i]);
                  if (!tmpRes) {
                    continue;
                  }
                  for (const item of tmpRes) {
                    newArr.push(item);
                  }
                }

                return newArr;
              });
            },
            { f, transformer },
          );
        },
        {
          transformer,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
    );
  }

  filter(filterFunc: (input: any) => boolean): RDD<T> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = this.getFunc();

    return new GeneratedRDD<T>(
      this._context,
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          return sf.captureEnv(
            () => {
              return Promise.resolve(f()).then(arr => {
                const newArr = [];
                for (let i = 0; i < arr.length; i++) {
                  if (filterFunc(arr[i])) {
                    newArr.push(arr[i]);
                  }
                }

                return newArr;
              });
            },
            { f, filterFunc },
          );
        },
        { filterFunc, sf: sf.requireModule('@dcfjs/common/serializeFunction') },
      ),
    );
  }

  reduce(reduceFunc: (a: T, b: T) => T): Promise<T | null> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = this.getFunc();

    return this._context.execute(
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          return sf.captureEnv(
            () => {
              return Promise.resolve(f()).then(arr => {
                if (arr.length < 2) {
                  return arr[0];
                }

                let lastRes = reduceFunc(arr[0], arr[1]);
                for (let i = 2; i < arr.length; i++) {
                  lastRes = reduceFunc(lastRes, arr[i]);
                }

                return lastRes;
              });
            },
            { f, reduceFunc },
          );
        },
        {
          partitionFunc,
          reduceFunc,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
      v => {
        v = v.filter(v => typeof v !== 'undefined');

        if (v.length === 1) {
          return v[0];
        } else if (v.length === 0) {
          return null;
        }

        let lastRes = reduceFunc(v[0], v[1]);
        for (let i = 2; i < v.length; i++) {
          lastRes = reduceFunc(lastRes, v[i]);
        }

        return lastRes;
      },
    );
  }
}

export class GeneratedRDD<T> extends RDD<T> {
  private _partitionCount: number;
  private _function?: PartitionFunc<T[]>;

  constructor(context: Context, partitionCount: 0);
  constructor(
    context: Context,
    partitionCount: number,
    func: (paritionId: number) => () => T[] | Promise<T[]>,
  );
  constructor(
    context: Context,
    partitionCount: number,
    func?: (paritionId: number) => () => T[] | Promise<T[]>,
  ) {
    super(context);
    this._partitionCount = partitionCount;
    this._function = func;
  }

  getFunc(): PartitionFunc<T[]> {
    return this._function!;
  }

  getNumPartitions(): number {
    return this._partitionCount;
  }
}
