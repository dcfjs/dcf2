import '@dcfjs/common/noCaptureEnv';
import { ExecTask } from './../master/index';
import { FunctionEnv } from './../common/serializeFunction';
import { WorkDispatcher } from '@dcfjs/master/workerManager';
import { Context } from './Context';
import ah = require('@dcfjs/common/arrayHelpers');
import sf = require('@dcfjs/common/serializeFunction');
import sr = require('@dcfjs/common/storageRegistry');
const v8 = require('v8');

export type ParallelTask = (dispachWorker: WorkDispatcher) => any;
export type PartitionFunc<T> = (
  paritionId: number,
) => (workerId: string) => T | Promise<T>;
export type MapperFunc<T, T1> = (input: T[]) => T1[];

let DeprecationWarningPrinted = false;
const envDeprecatedMsg =
  'DeprecationWarning: manual env passing is deprecated and will be removed in future. use `captureEnv` or `registerCaptureEnv` instead.\nSee also: https://dcf.gitbook.io/dcf/guide/serialized-function-since-2.0';

function envDeprecated() {
  if (!DeprecationWarningPrinted) {
    console.warn(envDeprecatedMsg);
    DeprecationWarningPrinted = true;
  }
}

export abstract class RDD<T> {
  protected _context: Context;
  protected constructor(context: Context) {
    this._context = context;
  }
  abstract getFunc(): PartitionFunc<T[]> | Promise<PartitionFunc<T[]>>;
  abstract getNumPartitions(): number;

  union(...others: RDD<T>[]): RDD<T> {
    return this._context.union(this, ...others);
  }

  async collect(): Promise<T[]> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = await this.getFunc();

    return this._context.execute(numPartitions, partitionFunc, ah.concatArrays);
  }

  async count(): Promise<number> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = await this.getFunc();

    return this._context.execute(
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          return sf.captureEnv(
            (workerId: string) => {
              return Promise.resolve(f(workerId)).then(v => v.length);
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

  async take(count: number): Promise<T[]> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = await this.getFunc();

    return this._context.execute(
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          return sf.captureEnv(
            (workerId: string) => {
              return Promise.resolve(f(workerId)).then(v => v.slice(0, count));
            },
            { f, count },
          );
        },
        {
          count,
          partitionFunc,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
      sf.captureEnv(v => ah.concatArrays(v).slice(0, count), {
        count,
        ah: sf.requireModule('@dcfjs/common/arrayHelpers'),
      }),
    );
  }

  max(
    this: RDD<number>,
    comparator?: (a: number, b: number) => number,
  ): Promise<T | undefined>;
  max(comparator: (a: T, b: T) => number): Promise<T | undefined>;
  max(comparator: any = ah.defaultComparator): Promise<T | undefined> {
    return this.reduce(
      sf.captureEnv(
        (a, b) => {
          return comparator(a, b) > 0 ? a : b;
        },
        {
          comparator,
        },
      ),
    );
  }

  min(
    this: RDD<number>,
    comparator?: (a: number, b: number) => number,
  ): Promise<T | undefined>;
  min(comparator: (a: T, b: T) => number): Promise<T | undefined>;
  min(comparator: any = ah.defaultComparator): Promise<T | undefined> {
    return this.reduce(
      sf.captureEnv(
        (a, b) => {
          return comparator(a, b) < 0 ? a : b;
        },
        {
          comparator,
        },
      ),
    );
  }

  mapPartitions<T1>(
    transformer: (input: T[]) => T1[],
    env?: FunctionEnv,
  ): RDD<T1> {
    if (env) {
      sf.captureEnv(transformer, env);
      envDeprecated();
    }
    return new MappedRDD<T1, T>(this._context, this, transformer);
  }

  glom(): RDD<T[]> {
    return this.mapPartitions(v => [v]);
  }

  map<T1>(transformer: (input: T) => T1, env?: FunctionEnv): RDD<T1> {
    if (env) {
      sf.captureEnv(transformer, env);
      envDeprecated();
    }
    return this.mapPartitions(
      sf.captureEnv(v => v.map(v => transformer(v)), { transformer }),
    );
  }

  flatMap<T1>(transformer: (input: T) => T1[], env?: FunctionEnv): RDD<T1> {
    if (env) {
      sf.captureEnv(transformer, env);
      envDeprecated();
    }
    return this.mapPartitions(
      sf.captureEnv(v => ah.concatArrays(v.map(v => transformer(v))), {
        transformer,
        ah: sf.requireModule('@dcfjs/common/arrayHelpers'),
      }),
    );
  }

  filter(filterFunc: (input: T) => boolean, env?: FunctionEnv): RDD<T> {
    if (env) {
      sf.captureEnv(filterFunc, env);
      envDeprecated();
    }
    return this.mapPartitions(
      sf.captureEnv(v => v.filter(v => filterFunc(v)), { filterFunc }),
    );
  }

  async reduce(
    reduceFunc: (a: T, b: T) => T,
    env?: any,
  ): Promise<T | undefined> {
    const numPartitions = this.getNumPartitions();
    const partitionFunc = await this.getFunc();

    if (env) {
      sf.captureEnv(reduceFunc, env);
      envDeprecated();
    }

    return this._context.execute(
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          return sf.captureEnv(
            workerId => {
              return Promise.resolve(f(workerId)).then(arr => {
                let lastRes: T | undefined = arr[0];

                for (let i = 1; i < arr.length; i++) {
                  lastRes = reduceFunc(lastRes, arr[i]);
                }

                return lastRes as T | undefined;
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
      sf.captureEnv(
        arr => {
          arr = arr.filter(v => v !== undefined) as T[];
          let lastRes: T | undefined = arr[0];
          for (let i = 1; i < arr.length; i++) {
            lastRes = reduceFunc(lastRes!, arr[i]!);
          }

          return lastRes;
        },
        {
          reduceFunc,
        },
      ),
    );
  }

  persist(storageType?: string): RDD<T> {
    return new CachedRDD(
      this._context,
      this,
      storageType || this._context.option.defaultStorage,
    );
  }
  // cache is only a alias of persist in dcf 2.0
  cache(storageType?: string): RDD<T> {
    return this.persist(storageType);
  }
}

export class GeneratedRDD<T> extends RDD<T> {
  private _numPartition: number;
  private _function?: PartitionFunc<T[]>;

  constructor(context: Context, numPartition: 0);
  constructor(
    context: Context,
    numPartition: number,
    func: (paritionId: number) => () => T[] | Promise<T[]>,
  );
  constructor(
    context: Context,
    numPartition: number,
    func?: (paritionId: number) => () => T[] | Promise<T[]>,
  ) {
    super(context);
    this._numPartition = numPartition;
    this._function = func;
  }

  getFunc(): PartitionFunc<T[]> {
    return this._function!;
  }

  getNumPartitions(): number {
    return this._numPartition;
  }
}

export class MappedRDD<T1, T> extends RDD<T1> {
  private _numPartition: number;
  private _mapper: MapperFunc<T, T1>;
  private _dependence: RDD<T>;

  constructor(context: Context, dependence: RDD<T>, mapper: MapperFunc<T, T1>) {
    super(context);

    this._numPartition = dependence.getNumPartitions();
    this._mapper = mapper;
    this._dependence = dependence;
  }

  async getFunc(): Promise<PartitionFunc<T1[]>> {
    const mapper = this._mapper;
    const depFunc = await this._dependence.getFunc();

    return sf.captureEnv(
      partitionId => {
        const f = depFunc(partitionId);
        return sf.captureEnv(
          (workerId: string) => {
            return Promise.resolve(f(workerId)).then(mapper);
          },
          {
            f,
            mapper,
          },
        );
      },
      {
        mapper,
        depFunc,
        sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      },
    );
  }

  getNumPartitions(): number {
    return this._numPartition;
  }
}

export class UnionRDD<T> extends RDD<T> {
  private _numPartition: number;
  private _dependences: RDD<T>[];

  constructor(context: Context, dependences: RDD<T>[]) {
    super(context);
    this._numPartition = dependences
      .map(v => v.getNumPartitions())
      .reduce((a, b) => a + b);
    this._dependences = dependences;
  }

  getNumPartitions(): number {
    return this._numPartition;
  }

  async getFunc(): Promise<PartitionFunc<T[]>> {
    const partitionCounts = this._dependences.map(v => v.getNumPartitions());
    const rddFuncs: PartitionFunc<T[]>[] = [];
    for (let i = 0; i < this._dependences.length; i++) {
      rddFuncs.push(await this._dependences[i].getFunc());
    }

    return sf.captureEnv(
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
    );
  }
}

export class CachedRDD<T> extends RDD<T> {
  private _numPartition: number;
  private _dependence: RDD<T>;
  private _cachedPartitions?: string[];
  private _storageType: string;

  constructor(context: Context, dependence: RDD<T>, storageType: string) {
    super(context);
    this._numPartition = dependence.getNumPartitions();
    this._dependence = dependence;
    this._storageType = storageType;
  }

  getNumPartitions(): number {
    return this._numPartition;
  }

  async unpersist(): Promise<void> {
    const partitions = this._cachedPartitions;
    const storageType = this._storageType;

    if (partitions) {
      this._cachedPartitions = undefined;
      this._context.client.post(
        '/exec',
        sf.serializeFunction(
          sf.captureEnv(
            ((_, _1, tempStorageSession) => {
              for (const partition of partitions) {
                tempStorageSession.release(storageType, partition);
              }
            }) as ExecTask,
            {
              partitions,
              storageType,
            },
          ),
        ),
      );
    }
  }

  async getFunc(): Promise<PartitionFunc<T[]>> {
    const storageType = this._storageType;

    if (!this._cachedPartitions) {
      const depFunc = await this._dependence.getFunc();
      // calc cached partitions.
      this._cachedPartitions = await this._context.execute(
        this._numPartition,
        sf.captureEnv(
          (partitionId, tempStorageSession) => {
            const f = depFunc(partitionId);
            return [
              sf.captureEnv(
                async (workerId: string) => {
                  const data = await f(workerId);
                  const storage = sr.getTempStorage(storageType);
                  const key = await storage.generateKey();
                  storage.setItem(key, v8.serialize(data));
                  return key;
                },
                {
                  sr: sf.requireModule('@dcfjs/common/storageRegistry'),
                  v8: sf.requireModule('v8'),
                  f,
                  storageType,
                },
              ),
              (key: string) => {
                tempStorageSession.addRef(storageType, key);
                return key;
              },
            ];
          },
          {
            sf: sf.requireModule('@dcfjs/common/serializeFunction'),
            storageType,
            depFunc,
          },
        ),
        arr => {
          return arr;
        },
      );
    }
    const partitions = this._cachedPartitions!;
    return sf.captureEnv(
      partitionId => {
        const partition = partitions[partitionId];
        return sf.captureEnv(
          () => {
            const storage = sr.getTempStorage(storageType);
            return v8.deserialize(storage.getItem(partition));
          },
          {
            storageType,
            partition,
            sr: sf.requireModule('@dcfjs/common/storageRegistry'),
            v8: sf.requireModule('v8'),
          },
        );
      },
      {
        partitions,
        storageType,
        sf: sf.requireModule('@dcfjs/common/serializeFunction'),
      },
    );
  }
}
