import { FinalizedFunc } from './RDD';
import '@dcfjs/common/noCaptureEnv';
import { ExecTask } from './../master/index';
import { FunctionEnv } from './../common/serializeFunction';
import { WorkDispatcher } from '@dcfjs/master/workerManager';
import { Context } from './Context';
import ah = require('@dcfjs/common/arrayHelpers');
import sf = require('@dcfjs/common/serializeFunction');
import sr = require('@dcfjs/common/storageRegistry');
const XXHash = require('xxhash');
const pack = require('@dcfjs/objpack');
const v8 = require('v8');

export type ParallelTask = (dispachWorker: WorkDispatcher) => any;
export type PartitionFunc<T> = (
  paritionId: number,
) => (workerId: string) => T | Promise<T>;
export type FinalizedFunc = (ts: sr.TempStorageSession) => void | Promise<void>;
export type MapperFunc<T, T1> = (input: T[]) => T1[];
export type ComparableType = string | number;
export type RDDFuncs<T> = [number, PartitionFunc<T>, FinalizedFunc | undefined];

let DeprecationWarningPrinted = false;
const envDeprecatedMsg =
  'DeprecationWarning: manual env passing is deprecated and will be removed in future. use `captureEnv` or `registerCaptureEnv` instead.\nSee also: https://dcf.gitbook.io/dcf/guide/serialized-function-since-2.0';

function envDeprecated() {
  if (!DeprecationWarningPrinted) {
    console.warn(envDeprecatedMsg);
    DeprecationWarningPrinted = true;
  }
}

function hashPartitionFunc<V>(numPartitions: number) {
  const seed = ((Math.random() * 0xffffffff) | 0) >>> 0;
  return sf.captureEnv(
    (data: V) => {
      return XXHash.hash(pack.encode(data), seed) % numPartitions;
    },
    {
      numPartitions,
      seed,
      XXHash: sf.requireModule('xxhash'),
      pack: sf.requireModule('@dcfjs/objpack'),
    },
  );
}

function realGroupWith<K>(
  rdds: RDD<[K, any]>[],
  context: Context,
  numPartitions?: number,
): RDD<[K, any[][]]> {
  const rddCount = rdds.length;

  return context
    .union(
      ...rdds.map((v, i) =>
        v.map(
          sf.captureEnv(
            ([k, v]) => {
              const ret: any[][] = [];
              for (let j = 0; j < rddCount; j++) {
                ret.push(j === i ? [v] : []);
              }
              return [k, ret] as [K, any[][]];
            },
            { rddCount, i },
          ),
        ),
      ),
    )
    .reduceByKey((a: any[][], b: any[][]) => {
      const ret = [];
      for (let i = 0; i < a.length; i++) {
        ret.push(a[i].concat(b[i]));
      }
      return ret;
    }, numPartitions);
}

function attachFinalizedFunc<T, T1>(
  func: (arg: T) => T1 | Promise<T1>,
  finalized?: FinalizedFunc,
): (arg: T, ts: sr.TempStorageSession) => T1 | Promise<T1> {
  if (!finalized) {
    return func;
  }
  return sf.captureEnv(
    async (arg: T, ts: sr.TempStorageSession) => {
      await finalized(ts);
      return func(arg);
    },
    {
      func,
      finalized,
    },
  );
}

export abstract class RDD<T> {
  protected _context: Context;
  protected constructor(context: Context) {
    this._context = context;
  }
  abstract getFunc(): RDDFuncs<T[]> | Promise<RDDFuncs<T[]>>;

  // Dependence rdd should override this method to get better performance.
  getNumPartitions(): number | Promise<number> {
    const tmp = this.getFunc();
    if (tmp instanceof Promise) {
      return tmp.then(v => v[0]);
    }
    return tmp[0];
  }

  union(...others: RDD<T>[]): RDD<T> {
    return this._context.union(this, ...others);
  }

  async collect(): Promise<T[]> {
    const [numPartitions, partitionFunc, finalizedFunc] = await this.getFunc();

    return this._context.execute(
      numPartitions,
      partitionFunc,
      attachFinalizedFunc(ah.concatArrays, finalizedFunc),
    );
  }

  async count(): Promise<number> {
    const [numPartitions, partitionFunc, finalizedFunc] = await this.getFunc();

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
      attachFinalizedFunc(v => v.reduce((a, b) => a + b, 0), finalizedFunc),
    );
  }

  async take(count: number): Promise<T[]> {
    const [numPartitions, partitionFunc, finalizedFunc] = await this.getFunc();

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
      attachFinalizedFunc(
        sf.captureEnv(v => ah.concatArrays(v).slice(0, count), {
          count,
          ah: sf.requireModule('@dcfjs/common/arrayHelpers'),
        }),
        finalizedFunc,
      ),
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
    env?: FunctionEnv,
  ): Promise<T | undefined> {
    const [numPartitions, partitionFunc, finalizedFunc] = await this.getFunc();

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
      attachFinalizedFunc(
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
        finalizedFunc,
      ),
    );
  }

  persist(storageType?: string): CachedRDD<T> {
    return new CachedRDD(
      this._context,
      this,
      storageType || this._context.option.defaultPersistStorage,
    );
  }
  // cache is only a alias of persist in dcf 2.0
  cache(storageType?: string): CachedRDD<T> {
    return this.persist(storageType);
  }

  partitionBy(
    numPartitions: number,
    partitionFunc: (v: T) => number,
    env?: FunctionEnv,
  ): RDD<T> {
    if (env) {
      sf.captureEnv(partitionFunc, env);
      envDeprecated();
    }
    return new RepartitionRDD(
      this._context,
      this,
      numPartitions,
      sf.captureEnv(
        partitionId =>
          sf.captureEnv(
            (data: T[]) => {
              const regrouped: T[][] = [];
              for (let i = 0; i < numPartitions; i++) {
                regrouped[i] = [];
              }

              for (const item of data) {
                const parId = partitionFunc(item);
                regrouped[parId].push(item);
              }
              return regrouped;
            },
            { numPartitions, partitionFunc },
          ),
        {
          numPartitions,
          partitionFunc,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
    );
  }

  coalesce(numPartitions: number): RDD<T> {
    return new CoalesceRDD<T>(this._context, this, numPartitions);
  }

  repartition(numPartitions: number): RDD<T> {
    return this.partitionBy(
      numPartitions,
      sf.captureEnv(() => (Math.random() * numPartitions) | 0, {
        numPartitions,
      }),
    );
  }

  distinct(
    numPartitions: number = this._context.option.defaultPartitions,
  ): RDD<T> {
    return this.partitionBy(
      numPartitions,
      hashPartitionFunc<T>(numPartitions),
    ).mapPartitions(
      sf.captureEnv(
        datas => {
          const ret = [];
          const map: { [key: string]: T } = {};
          for (const item of datas) {
            const k = pack.encode(item).toString('base64');
            if (!map[k]) {
              map[k] = item;
              ret.push(item);
            }
          }
          return ret;
        },
        {
          pack: sf.requireModule('@dcfjs/objpack'),
        },
      ),
    );
  }

  combineByKey<K, V, C>(
    this: RDD<[K, V]>,
    createCombiner: (a: V) => C,
    mergeValue: (a: C, b: V) => C,
    mergeCombiners: (a: C, b: C) => C,
    numPartitions: number = this._context.option.defaultPartitions,
    partitionFunc: (v: K) => number = hashPartitionFunc<K>(numPartitions),
    env?: FunctionEnv,
  ): RDD<[K, C]> {
    if (env) {
      sf.captureEnv(createCombiner, env);
      sf.captureEnv(mergeValue, env);
      sf.captureEnv(mergeCombiners, env);
      sf.captureEnv(partitionFunc, env);
      envDeprecated();
    }

    const mapFunction1 = sf.captureEnv(
      (datas: [K, V][]) => {
        const ret = [];
        const map: { [key: string]: [K, C] } = {};
        for (const item of datas) {
          const k = pack.encode(item[0]).toString('base64');
          let r = map[k];
          if (!r) {
            r = [item[0], createCombiner(item[1])];
            map[k] = r;
            ret.push(r);
          } else {
            r[1] = mergeValue(r[1], item[1]);
          }
        }
        return ret;
      },
      {
        createCombiner,
        mergeValue,
        pack: sf.requireModule('@dcfjs/objpack'),
      },
    );

    const mapFunction2 = sf.captureEnv(
      (datas: [K, C][]) => {
        const ret = [];
        const map: { [key: string]: [K, C] } = {};
        for (const item of datas) {
          const k = pack.encode(item[0]).toString('base64');
          let r = map[k];
          if (!r) {
            r = [item[0], item[1]];
            map[k] = r;
            ret.push(r);
          } else {
            r[1] = mergeCombiners(r[1], item[1]);
          }
        }
        return ret;
      },
      {
        mergeCombiners,
        pack: sf.requireModule('@dcfjs/objpack'),
      },
    );

    const realPartitionFunc = sf.captureEnv(
      (data: [K, C]) => {
        return partitionFunc(data[0]);
      },
      {
        partitionFunc,
      },
    );

    return this.mapPartitions<[K, C]>(mapFunction1)
      .partitionBy(numPartitions, realPartitionFunc)
      .mapPartitions<[K, C]>(mapFunction2);
  }

  reduceByKey<K, V>(
    this: RDD<[K, V]>,
    func: (a: V, B: V) => V,
    numPartitions: number = this._context.option.defaultPartitions,
    partitionFunc?: (v: K) => number,
    env?: FunctionEnv,
  ): RDD<[K, V]> {
    return this.combineByKey(
      x => x,
      func,
      func,
      numPartitions,
      partitionFunc,
      env,
    );
  }

  groupWith<K, V, V1>(
    this: RDD<[K, V]>,
    other1: RDD<[K, V1]>,
  ): RDD<[K, [V[], V1[]]]>;
  groupWith<K, V, V1, V2>(
    this: RDD<[K, V]>,
    other1: RDD<[K, V1]>,
    other2: RDD<[K, V2]>,
  ): RDD<[K, [V[], V1[], V2[]]]>;
  groupWith<K, V, V1, V2, V3>(
    this: RDD<[K, V]>,
    other1: RDD<[K, V1]>,
    other2: RDD<[K, V2]>,
    other3: RDD<[K, V3]>,
  ): RDD<[K, [V[], V1[], V2[], V3[]]]>;
  groupWith<K>(this: RDD<[K, any]>, ...others: RDD<[K, any]>[]): RDD<[K, any]>;
  groupWith<K>(this: RDD<[K, any]>, ...others: RDD<[K, any]>[]): RDD<[K, any]> {
    return realGroupWith([this, ...others], this._context);
  }

  cogroup<K, V, V1>(
    this: RDD<[K, V]>,
    other: RDD<[K, V1]>,
    numPartitions?: number,
  ): RDD<[K, [V[], V1[]]]> {
    return realGroupWith([this, other], this._context, numPartitions) as RDD<
      [K, [V[], V1[]]]
      >;
  }

  join<K, V, V1>(
    this: RDD<[K, V]>,
    other: RDD<[K, V1]>,
    numPartitions?: number,
  ): RDD<[K, [V, V1]]> {
    return this.cogroup(other, numPartitions).flatMap(([k, [v1s, v2s]]) => {
      const ret = [];
      for (const v1 of v1s) {
        for (const v2 of v2s) {
          ret.push([k, [v1, v2]] as [K, [V, V1]]);
        }
      }
      return ret;
    });
  }

  leftOuterJoin<K, V, V1>(
    this: RDD<[K, V]>,
    other: RDD<[K, V1]>,
    numPartitions?: number,
  ): RDD<[K, [V, V1 | null]]> {
    return this.cogroup(other, numPartitions).flatMap(([k, [v1s, v2s]]) => {
      const ret = [];
      if (v2s.length === 0) {
        for (const v1 of v1s) {
          ret.push([k, [v1, null]] as [K, [V, V1 | null]]);
        }
      } else {
        for (const v1 of v1s) {
          for (const v2 of v2s) {
            ret.push([k, [v1, v2]] as [K, [V, V1 | null]]);
          }
        }
      }
      return ret;
    });
  }

  rightOuterJoin<K, V, V1>(
    this: RDD<[K, V]>,
    other: RDD<[K, V1]>,
    numPartitions?: number,
  ): RDD<[K, [V | null, V1]]> {
    return this.cogroup(other, numPartitions).flatMap(([k, [v1s, v2s]]) => {
      const ret = [];
      if (v1s.length === 0) {
        for (const v2 of v2s) {
          ret.push([k, [null, v2]] as [K, [V | null, V1]]);
        }
      } else {
        for (const v1 of v1s) {
          for (const v2 of v2s) {
            ret.push([k, [v1, v2]] as [K, [V | null, V1]]);
          }
        }
      }
      return ret;
    });
  }

  fullOuterJoin<K, V, V1>(
    this: RDD<[K, V]>,
    other: RDD<[K, V1]>,
    numPartitions?: number,
  ): RDD<[K, [V | null, V1 | null]]> {
    return this.cogroup(other, numPartitions).flatMap(([k, [v1s, v2s]]) => {
      const ret = [];
      if (v1s.length === 0) {
        for (const v2 of v2s) {
          ret.push([k, [null, v2]] as [K, [V | null, V1 | null]]);
        }
      } else if (v2s.length === 0) {
        for (const v1 of v1s) {
          ret.push([k, [v1, null]] as [K, [V | null, V1 | null]]);
        }
      } else {
        for (const v1 of v1s) {
          for (const v2 of v2s) {
            ret.push([k, [v1, v2]] as [K, [V | null, V1 | null]]);
          }
        }
      }
      return ret;
    });
  }

  sort<K extends ComparableType>(
    this: RDD<K>,
    ascending: boolean = true,
    numPartitions?: number,
  ) {
    return this.sortBy(v => v, ascending, numPartitions);
  }

  sortBy<K extends ComparableType>(
    keyFunc: (data: T) => K,
    ascending: boolean = true,
    numPartitions?: number,
    env?: FunctionEnv,
  ): RDD<T> {
    return new SortedRDD<T, K>(
      this._context,
      this,
      numPartitions || this._context.option.defaultPartitions,
      keyFunc,
      ascending,
    );
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

  getFunc(): RDDFuncs<T[]> {
    return [this._numPartition, this._function!, undefined];
  }

  getNumPartitions() {
    return this._numPartition;
  }
}

export class MappedRDD<T1, T> extends RDD<T1> {
  private _mapper: MapperFunc<T, T1>;
  private _dependence: RDD<T>;

  constructor(context: Context, dependence: RDD<T>, mapper: MapperFunc<T, T1>) {
    super(context);

    this._mapper = mapper;
    this._dependence = dependence;
  }

  async getFunc(): Promise<RDDFuncs<T1[]>> {
    const mapper = this._mapper;
    const [
      numPartitions,
      depFunc,
      depFinalizer,
    ] = await this._dependence.getFunc();

    return [
      numPartitions,
      sf.captureEnv(
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
      ),
      depFinalizer,
    ];
  }

  getNumPartitions() {
    return this._dependence.getNumPartitions();
  }
}

export class UnionRDD<T> extends RDD<T> {
  private _dependences: RDD<T>[];

  constructor(context: Context, dependences: RDD<T>[]) {
    super(context);
    this._dependences = dependences;
  }

  async getFunc(): Promise<RDDFuncs<T[]>> {
    const partitionCounts: number[] = [];
    const rddFuncs: PartitionFunc<T[]>[] = [];
    const rddFinalizers: FinalizedFunc[] = [];
    for (let i = 0; i < this._dependences.length; i++) {
      const [numPartitions, func, finalizer] = await this._dependences[
        i
        ].getFunc();
      partitionCounts.push(numPartitions);
      rddFuncs.push(func);
      if (finalizer) {
        rddFinalizers.push(finalizer);
      }
    }

    const numPartitions = partitionCounts.reduce((a, b) => a + b);

    return [
      numPartitions,
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
      rddFinalizers.length === 0
        ? undefined
        : sf.captureEnv(
        async (ts: sr.TempStorageSession) => {
          for (const item of rddFinalizers) {
            await item(ts);
          }
        },
        { rddFinalizers },
        ),
    ];
  }

  getNumPartitions() {
    const partitionCounts = this._dependences.map(v => v.getNumPartitions());

    if (partitionCounts.some(v => v instanceof Promise)) {
      return Promise.all(partitionCounts).then(v => v.reduce((a, b) => a + b));
    }
    return (partitionCounts as number[]).reduce((a, b) => a + b);
  }
}

export class CachedRDD<T> extends RDD<T> {
  private _dependence: RDD<T>;
  private _cachedNumPartition?: number;
  private _cachedPartitions?: string[];
  private _storageType: string;
  private _autoUnpersist: boolean = false;

  constructor(context: Context, dependence: RDD<T>, storageType: string) {
    super(context);
    this._dependence = dependence;
    this._storageType = storageType;
  }

  // mark this cache auto unpersist after used again.
  // Used to clean up a temporary before last use.
  autoUnpersist() {
    this._autoUnpersist = true;
  }

  async unpersist(): Promise<void> {
    const partitions = this._cachedPartitions;
    const storageType = this._storageType;

    if (partitions) {
      this._cachedPartitions = undefined;
      this._context.client.post<void>(
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

  async getFunc(): Promise<RDDFuncs<T[]>> {
    const storageType = this._storageType;

    if (!this._cachedPartitions) {
      if (this._autoUnpersist) {
        // auto unpersist + not cached, use dependence directly.
        return await this._dependence.getFunc();
      }
      const [
        numPartitions,
        depFunc,
        depFinalizer,
      ] = await this._dependence.getFunc();
      this._cachedNumPartition = numPartitions;
      // calc cached partitions.
      this._cachedPartitions = await this._context.execute(
        numPartitions,
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
        attachFinalizedFunc(arr => arr, depFinalizer),
      );
    }

    const autoUnpersist = this._autoUnpersist;

    const partitions = this._cachedPartitions!;
    return [
      this._cachedNumPartition!,
      sf.captureEnv(
        partitionId => {
          const partition = partitions[partitionId];
          return sf.captureEnv(
            async () => {
              const storage = sr.getTempStorage(storageType);
              return v8.deserialize(
                autoUnpersist
                  ? await storage.getAndDeleteItem(partition)
                  : await storage.getItem(partition),
              );
            },
            {
              autoUnpersist,
              storageType,
              partition,
              sr: sf.requireModule('@dcfjs/common/storageRegistry'),
              v8: sf.requireModule('v8'),
            },
          );
        },
        {
          autoUnpersist,
          partitions,
          storageType,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
      autoUnpersist
        ? sf.captureEnv(
        (ts: sr.TempStorageSession) => {
          for (const partition of partitions) {
            ts.release(storageType, partition);
          }
        },
        {
          partitions,
          storageType,
        },
        )
        : undefined,
    ];
  }

  getNumPartitions() {
    if (this._cachedPartitions) {
      return this._cachedNumPartition!;
    }
    return this._dependence.getNumPartitions();
  }
}

async function getRepartitionFunc<T, T1 = T>(
  context: Context,
  dependence: RDDFuncs<T[]>,
  numPartitions: number,
  partitionFunc: (paritionId: number) => (v: T[]) => T1[][],
): Promise<RDDFuncs<T1[]>> {
  const storageType = context.option.defaultRepartitionStorage;

  const [depPartitions, depFunc, depFinalizer] = dependence;

  const repartitionId = await context.client.post<string>(
    '/exec',
    sf.serializeFunction(
      sf.captureEnv(
        (() => {
          return sr.getTempStorage(storageType).generateKey();
        }) as ExecTask,
        {
          sr: sf.requireModule('@dcfjs/common/storageRegistry'),
          storageType,
        },
      ),
    ),
  );

  const pieces = await context.execute(
    depPartitions,
    sf.captureEnv(
      (partitionId, tempStorageSession) => {
        const f = depFunc(partitionId);
        const pf = partitionFunc(partitionId);
        return [
          sf.captureEnv(
            async (workerId: string) => {
              const data = await f(workerId);
              const storage = sr.getTempStorage(storageType);

              const regrouped: T1[][] = pf(data);

              const ret: (string | null)[] = [];
              for (let i = 0; i < numPartitions; i++) {
                if (!regrouped[i] || regrouped[i].length === 0) {
                  ret.push(null);
                  continue;
                }
                const key = `${repartitionId}-${workerId}-${i}`;
                const buf: Buffer = v8.serialize(regrouped[i]);
                const len = Buffer.allocUnsafe(4);
                len.writeUInt32LE(buf.length, 0);

                await storage.appendItem(key, Buffer.concat([len, buf]));

                ret.push(key);
              }

              return ret;
            },
            {
              f,
              pf,
              numPartitions,
              repartitionId,
              storageType,
              sr: sf.requireModule('@dcfjs/common/storageRegistry'),
              v8: sf.requireModule('v8'),
            },
          ),
          (keys: (string | null)[]) => {
            for (const key of keys) {
              if (key) {
                tempStorageSession.addRef(storageType, key);
              }
            }
            return keys;
          },
        ];
      },
      {
        sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        numPartitions,
        repartitionId,
        partitionFunc,
        storageType,
        depFunc,
      },
    ),
    attachFinalizedFunc(
      sf.captureEnv(
        arr => {
          // transposition matrix with new partition
          const ret: (string | null)[][] = [];
          for (let i = 0; i < numPartitions; i++) {
            ret[i] = [];
            for (let j = 0; j < depPartitions; j++) {
              ret[i][j] = arr[j][i];
            }
          }
          return ret;
        },
        {
          numPartitions,
          depPartitions,
        },
      ),
      depFinalizer,
    ),
  );

  return [
    numPartitions,
    sf.captureEnv(
      partitionId => {
        const partPieces = pieces[partitionId];
        // TODO: use execution context to release references.

        return sf.captureEnv(
          async () => {
            const storage = sr.getTempStorage(storageType);
            const dataByKey: { [key: string]: T1[][] } = {};
            for (let i = 0; i < partPieces.length; i++) {
              const key = partPieces[i];
              if (key && !dataByKey[key]) {
                const allBuf = await storage.getAndDeleteItem(key);
                let position = 0;
                const tmp: T1[][] = [];
                while (position < allBuf.length) {
                  const len = allBuf.readUInt32LE(position);
                  position += 4;
                  const buf = allBuf.slice(position, position + len);
                  position += len;
                  tmp.push(v8.deserialize(buf));
                }
                dataByKey[key] = tmp;
              }
            }
            const parts: T1[][] = [];
            for (let i = 0; i < partPieces.length; i++) {
              const key = partPieces[i];
              if (key) {
                parts.push(dataByKey[key].shift()!);
              }
            }
            return ah.concatArrays(parts);
          },
          {
            partPieces,
            storageType,
            depPartitions,
            sr: sf.requireModule('@dcfjs/common/storageRegistry'),
            v8: sf.requireModule('v8'),
            ah: sf.requireModule('@dcfjs/common/arrayHelpers'),
          },
        );
      },
      {
        sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        storageType,
        numPartitions,
        depPartitions,
        pieces,
      },
    ),
    sf.captureEnv(
      (ts: sr.TempStorageSession) => {
        for (const partition of pieces) {
          for (const piece of partition) {
            if (piece) {
              ts.release(storageType, piece);
            }
          }
        }
      },
      {
        pieces,
        storageType,
      },
    ),
  ];
}

export class RepartitionRDD<T> extends RDD<T> {
  private _numPartition: number;
  private _dependence: RDD<T>;
  private _partitionFunc: (paritionId: number) => (v: T[]) => T[][];
  constructor(
    context: Context,
    dependence: RDD<T>,
    numPartition: number,
    partitionFunc: (paritionId: number) => (v: T[]) => T[][],
  ) {
    super(context);
    this._numPartition = numPartition;
    this._dependence = dependence;
    this._partitionFunc = partitionFunc;
  }

  async getFunc(): Promise<RDDFuncs<T[]>> {
    return getRepartitionFunc(
      this._context,
      await this._dependence.getFunc(),
      this._numPartition,
      this._partitionFunc,
    );
  }

  getNumPartitions() {
    return this._numPartition;
  }
}

export class SortedRDD<T, K extends ComparableType> extends RDD<T> {
  private _numPartition: number;
  private _dependence: RDD<T>;
  private _keyFunc: (data: T) => K;
  private _ascending: boolean;

  constructor(
    context: Context,
    dependence: RDD<T>,
    numPartition: number,
    keyFunc: (data: T) => K,
    ascending: boolean,
  ) {
    super(context);
    this._numPartition = numPartition;
    this._dependence = dependence;
    this._numPartition = numPartition;
    this._keyFunc = keyFunc;
    this._ascending = ascending;
  }

  async getFunc(): Promise<RDDFuncs<T[]>> {
    let cachedDependence: CachedRDD<T>;
    const storageType = this._context.option.defaultRepartitionStorage;

    // Step1: cache if needed.
    if (this._dependence instanceof CachedRDD) {
      cachedDependence = this._dependence;
    } else {
      cachedDependence = this._dependence.persist(storageType) as CachedRDD<T>;
    }

    // Step 2:
    // sample with 1/n fraction where n is origin partition count
    // so we get a sample count near to a single partition.
    // Sample contains partitionIndex & localIndex to avoid performance inssue
    // when dealing with too many same values.
    const [
      originPartitions,
      originFunc,
      originFinalizer,
    ] = await cachedDependence.getFunc();

    const keyFunc = this._keyFunc;

    const samples = await this._context.execute(
      originPartitions,
      sf.captureEnv(
        partitionId => {
          const f = originFunc(partitionId);

          return sf.captureEnv(
            async (workerId: string) => {
              const v = await f(workerId);
              const ret = [];
              for (let i = 0; i < v.length; i++) {
                if (Math.random() * originPartitions < 1) {
                  ret.push([keyFunc(v[i]), partitionId, i] as [
                    K,
                    number,
                    number
                    ]);
                }
              }
              return ret;
            },
            { f, partitionId, keyFunc, originPartitions },
          );
        },
        {
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
          originPartitions,
          originFunc,
          keyFunc,
        },
      ),
      attachFinalizedFunc(
        sf.captureEnv(
          arr => {
            return ah.concatArrays(arr);
          },
          {
            ah: sf.requireModule('@dcfjs/common/arrayHelpers'),
          },
        ),
        originFinalizer,
      ),
    );

    const ascending = this._ascending;

    // Step 3: sort samples, and get seperate points.
    samples.sort((a, b) => {
      if (a[0] !== b[0]) {
        return (a[0] < b[0] ? -1 : 1) * (ascending ? 1 : -1);
      }
      if (a[1] !== b[1]) {
        return a[1] < b[1] ? -1 : 1;
      }
      return a[2] < b[2] ? -1 : 1;
    });

    // get n-1 points in samples.
    const numPartitions = this._numPartition;
    const points: [any, number, number][] = [];
    let p = samples.length / numPartitions;
    for (let i = 1; i < numPartitions; i++) {
      let idx = Math.floor(p * i);
      if (idx < samples.length) {
        points.push(samples[idx]);
      }
    }

    // Step 4: Repartition by points.

    if (this._dependence !== cachedDependence) {
      // remove persisted data after sorted.
      cachedDependence.autoUnpersist();
    }

    const [_, repartitionedFunc, finalizer] = await getRepartitionFunc(
      this._context,
      [originPartitions, originFunc, originFinalizer],
      numPartitions,
      sf.captureEnv(
        (partitionId: number) => {
          return sf.captureEnv(
            (v: T[]) => {
              const ret: [T, K, number, number][][] = [];
              for (let i = 0; i < numPartitions; i++) {
                ret.push([]);
              }
              const tmp = v.map((item, i) => [keyFunc(item), i] as [K, number]);
              tmp.sort((a, b) => {
                if (a[0] !== b[0]) {
                  return (a[0] < b[0] ? -1 : 1) * (ascending ? 1 : -1);
                }
                return a[1] < b[1] ? -1 : 1;
              });
              let index = 0;
              for (const [key, i] of tmp) {
                // compare with points
                for (; index < points.length; ) {
                  if (
                    ascending ? key < points[index][0] : key > points[index][0]
                  ) {
                    break;
                  }
                  if (
                    ascending ? key > points[index][0] : key < points[index][0]
                  ) {
                    index++;
                    continue;
                  }
                  if (partitionId < points[index][1]) {
                    break;
                  }
                  if (partitionId > points[index][1]) {
                    index++;
                    continue;
                  }
                  if (i < points[index][2]) {
                    break;
                  }
                  index++;
                }
                ret[index].push([v[i], key, partitionId, i]);
              }
              return ret;
            },
            {
              keyFunc,
              numPartitions,
              partitionId,
              points,
              ascending,
            },
          );
        },
        {
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
          keyFunc,
          numPartitions,
          points,
          ascending,
        },
      ),
    );

    // Step 5: Sort in partition.
    // Maybe reverse (if ascending == false).
    return [
      numPartitions,
      sf.captureEnv(
        (partitionId: number) => {
          const f = repartitionedFunc(partitionId);
          return sf.captureEnv(
            async (workerId: string) => {
              const tmp = await f(workerId);
              tmp.sort((a, b) => {
                if (a[1] !== b[1]) {
                  return (a[1] < b[1] ? -1 : 1) * (ascending ? 1 : -1);
                }
                if (a[2] !== b[2]) {
                  return a[2] < b[2] ? -1 : 1;
                }
                return a[3] < b[3] ? -1 : 1;
              });
              return tmp.map(item => item[0]);
            },
            {
              f,
              ascending,
            },
          );
        },
        {
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
          numPartitions,
          repartitionedFunc,
          ascending,
        },
      ),
      finalizer,
    ];
  }

  getNumPartitions() {
    return this._numPartition;
  }
}

export class CoalesceRDD<T> extends RDD<T> {
  private _numPartition: number;
  private _dependence: RDD<T>;
  constructor(context: Context, dependence: RDD<T>, numPartition: number) {
    super(context);
    this._numPartition = numPartition;
    this._dependence = dependence;
  }

  async getFunc(): Promise<RDDFuncs<T[]>> {
    const [
      originPartitions,
      originFunc,
      originFinalizer,
    ] = await this._dependence.getFunc();

    const numPartitions = this._numPartition;

    const partitionArgs: [number, number[]][] = [];
    let last: number[] = [];
    partitionArgs.push([0, last]);
    const rate = originPartitions / numPartitions;

    let counter = 0;
    for (let i = 0; i < numPartitions - 1; i++) {
      counter += rate;
      while (counter >= 1) {
        counter -= 1;
        last = [];
        partitionArgs.push([i, last]);
      }
      last.push(counter);
    }
    // manually add last partition to avoid precsion loss.
    while (partitionArgs.length < originPartitions) {
      partitionArgs.push([numPartitions - 1, []]);
    }

    return getRepartitionFunc(
      this._context,
      [originPartitions, originFunc, originFinalizer],
      numPartitions,
      sf.captureEnv(
        partitionId => {
          const arg = partitionArgs[partitionId];
          return sf.captureEnv(
            (data: T[]) => {
              const ret: any[][] = [];
              let lastIndex = 0;
              for (let i = 0; i < arg[0]; i++) {
                ret.push([]);
              }
              for (const rate of arg[1]) {
                const nextIndex = Math.floor(data.length * rate);
                ret.push(data.slice(lastIndex, nextIndex));
                lastIndex = nextIndex;
              }
              ret.push(data.slice(lastIndex));
              return ret;
            },
            { numPartitions, arg },
          );
        },
        {
          numPartitions,
          partitionArgs,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
    );
  }

  getNumPartitions() {
    return this._numPartition;
  }
}

export class LoadedFileRDD<T> extends RDD<T> {
  private _function: PartitionFunc<T[]>;
  private _fileListFunc: () => string[];
  private _numPartition: number;

  constructor(
    context: Context,
    partFunc: (paritionId: number) => () => T[] | Promise<T[]>,
    fileListFunc: () => string[],
  );
  constructor(
    context: Context,
    partFunc: (paritionId: number) => () => T[] | Promise<T[]>,
    fileListFunc: () => string[],
  ) {
    super(context);

    this._numPartition = 0;

    this._function = partFunc;
    this._fileListFunc = fileListFunc;
  }

  async getFunc(): Promise<RDDFuncs<T[]>> {
    const fileList = await this._fileListFunc();
    this._numPartition = fileList.length;

    return [this._numPartition, this._function!, undefined];
  }

  getNumPartitions() {
    return this._numPartition > 0 ? this._numPartition : 1;
  }
}
