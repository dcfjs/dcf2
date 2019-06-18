import { WorkDispatcher } from '@dcfjs/master/workerManager';
import { Context } from './Context';
import concatArrays from '@dcfjs/common/concatArrays';
import sf = require('@dcfjs/common/serializeFunction');

export type ParallelTask = (dispachWorker: WorkDispatcher) => any;
export type PartitionFunc<T> = (paritionId: number) => () => T;

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
              return f().length;
            },
            { f },
          ) as () => number;
        },
        {
          partitionFunc,
          sf: sf.requireModule('@dcfjs/common/serializeFunction'),
        },
      ),
      v => v.reduce((a, b) => a + b, 0),
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
    func: (paritionId: number) => () => T[],
  );
  constructor(
    context: Context,
    partitionCount: number,
    func?: (paritionId: number) => () => T[],
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
