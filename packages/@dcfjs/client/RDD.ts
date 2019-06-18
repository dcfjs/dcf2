import { WorkDispatcher } from '@dcfjs/master/workerManager';
import { Context } from './Context';
import concatArrays from '@dcfjs/common/concatArrays';
import { captureEnv } from '@dcfjs/common/serializeFunction';

export type ParallelTask = (dispachWorker: WorkDispatcher) => any;
export type PartitionFunc<T> = (paritionId: number) => () => T;

export class RDD<T> {
  protected _context: Context;
  protected constructor(context: Context) {
    this._context = context;
  }
  protected getFunc(): [number, PartitionFunc<T[]>] {
    throw new Error('Must be overrided.');
  }

  collect(): Promise<T[]> {
    const [numPartitions, partitionFunc] = this.getFunc();

    return this._context.execute(numPartitions, partitionFunc, concatArrays);
  }

  count(): Promise<number> {
    const [numPartitions, partitionFunc] = this.getFunc();

    return this._context.execute(
      numPartitions,
      captureEnv(
        partitionId => {
          const f = partitionFunc(partitionId);
          // TODO: This is ugly.
          return (global as any).__captureEnv(
            () => {
              return f().length;
            },
            { f },
          ) as () => number;
        },
        {
          partitionFunc,
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

  protected getFunc(): [number, PartitionFunc<T[]>] {
    return [this._partitionCount, this._function!];
  }
}
