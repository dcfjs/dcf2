import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import { releaseAll, autoRelease } from '@dcfjs/common/autoRelease';
import { createMasterServer } from '@dcfjs/master';
import { createLocalWorker } from '@dcfjs/worker';
import { Context, createContext } from '@dcfjs/client/Context';
import { RDD } from '@dcfjs/client';
import { expect, assert } from 'chai';

chai.use(chaiAsPromised);

function isPrime(v: number) {
  if (v < 2) {
    return false;
  }
  const max = Math.sqrt(v) + 0.5;
  for (let i = 2; i < max; i++) {
    if (v % i === 0) {
      return false;
    }
  }
  return true;
}

function toSet(v: any) {
  return new Set([v]);
}

function addToSet(v1: Set<any>, v2: any) {
  return v1.add(v2);
}

function mergeSet(v1: Set<any>, v2: Set<any>) {
  return new Set([...v1, ...v2]);
}

function toArray(v: any) {
  return [v];
}

function addToArray(x: any[], v: any) {
  x.push(v);
  return x;
}

function concatArray(x1: any[], x2: any[]) {
  return x1.concat(x2);
}

describe('MapReduce With local worker and sharedfs temp storage', () => {
  let dcc: Context;

  before(async () => {
    const server = await createMasterServer({
      storages: [
        {
          name: 'disk',
          module: '@dcfjs/common/SharedFsTempStorage',
        },
      ],
    });
    autoRelease(() => server.close());

    for (let i = 0; i < 4; i++) {
      autoRelease(await createLocalWorker(server.endpoint));
    }

    dcc = await createContext(server.endpoint);
    autoRelease(() => dcc.close());
  });

  after(async () => {
    await releaseAll();
    context = null!;
  });

  it('Test persist', async () => {
    const primes1 = dcc.range(0, 100000).filter(isPrime);

    const primes2 = primes1.persist('disk');

    expect(await primes1.count()).equals(await primes2.count());
    expect(await primes1.collect()).deep.equals(await primes2.collect());
  });

  it('Test repartitionBy', async () => {
    const max = 100000;
    const tmp = dcc
      .range(0, max)
      .partitionBy(3, v => v % 3)
      .cache();

    expect(await tmp.count()).equals(max);
    const compare: number[][] = [];
    for (let i = 0; i < 3; i++) {
      compare[i] = [];
      for (let j = 0; j < max; j++) {
        if (j % 3 === i) {
          compare[i].push(j);
        }
      }
    }
    expect(await tmp.glom().collect()).deep.equals(compare);
  });

  it('Test coalesce', async () => {
    const max = 10000;
    const tmp = dcc.range(0, max, undefined, 4);
    const tmp1 = tmp.coalesce(5);

    expect(await tmp.collect()).deep.equals(await tmp1.collect());
    expect((await tmp1.getNumPartitions()) === 5);
    const sizes = await tmp1
      .glom()
      .map(v => v.length)
      .collect();
    assert(sizes.every(v => v >= 1990 && v <= 2010));
  });

  it('Test repartition', async () => {
    const max = 100000;
    const tmp = dcc.range(0, max).repartition(16);

    expect(await tmp.count()).equals(max);
    expect(await tmp.max()).equals(max - 1);
    expect(await tmp.min()).equals(0);
    expect(await tmp.reduce((a, b) => a + b)).equals(4999950000);
  });

  it('Test distinct', async () => {
    const max = 100000;
    const tmp = dcc
      .range(0, max)
      .map(v => 1)
      .distinct();
    expect(await tmp.collect()).deep.equals([1]);

    const tmp2 = dcc.range(0, max / 10).map(v => 4);
    const tmp3 = dcc.range(0, max / 10).map(v => 8);
    const tmp4 = dcc.range(0, max / 10).filter(v => false);
    const tmp5 = tmp2
      .union(tmp3)
      .union(tmp4)
      .distinct();
    const res = await tmp5.collect();

    expect(res.sort()).deep.equals([4, 8]);
  });

  it('Test combineByKey', async function() {
    this.timeout(20000);

    const max = 100000;
    const tmp = dcc
      .range(0, max)
      .map(v => {
        const key = v % 4;
        return [key, v] as [number, number];
      })
      .combineByKey(toArray, addToArray, concatArray)
      .map(v => [v[0], v[1].sort((a, b) => (a | 0) - (b | 0))]);

    const res = (await tmp.collect()).sort(
      (a, b) => (a[0] as number) - (b[0] as number),
    );

    const tester = new Array(4);
    for (let i = 0; i < 4; i++) {
      tester[i] = [i, []];
    }
    for (let i = 0; i < max; i++) {
      const key = i % 4;
      tester[key][1].push(i);
    }

    expect(res).deep.equals(tester);

    const tmp2 = dcc
      .range(0, max)
      .map(v => {
        const key = v % 4;
        return [key, v] as [number, number];
      })
      .combineByKey(toSet, addToSet, mergeSet);
    const res2 = (await tmp2.collect()).sort(
      (a, b) => (a[0] as number) - (b[0] as number),
    );
    const tester2 = new Array(4);
    for (let i = 0; i < 4; i++) {
      tester2[i] = [i, new Set()];
    }
    for (let i = 0; i < max; i++) {
      const key = i % 4;
      tester2[key][1].add(i);
    }
    expect(res2).deep.equals(tester2);
  });

  it('Test reduceByKey', async () => {
    const max = 100000;
    const tmp = dcc
      .range(0, max)
      .map(v => {
        const key = v % 4;
        return [key, 1] as [number, number];
      })
      .reduceByKey((a, b) => a + b);

    const res = (await tmp.collect()).sort(
      (a, b) => (a[0] as number) - (b[0] as number),
    );

    const tester = new Array(4);
    for (let i = 0; i < 4; i++) {
      tester[i] = [i, max / 4];
    }

    expect(res).deep.equals(tester);
  });

  it('Test groupWith', async () => {
    const t1 = dcc.parallelize([['a', 5], ['a', 7], ['b', 6]]) as RDD<
      [any, any]
    >;
    const t2 = dcc.parallelize([['a', 1], ['b', 4]]) as RDD<[any, any]>;
    const t3 = dcc.parallelize([['a', 2]]) as RDD<[any, any]>;
    const t4 = dcc.parallelize([['b', 42], ['b', 66]]) as RDD<[any, any]>;
    const t5 = dcc.emptyRDD() as RDD<[any, any]>;
    let res = await t1.groupWith(t2, t3, t4, t5).collect();

    res = res.sort((a, b) => a[0].localeCompare(b[0]));

    expect(res).deep.equals([
      ['a', [[5, 7], [1], [2], [], []]],
      ['b', [[6], [4], [], [42, 66], []]],
    ]);
  });

  it('Test cogroup', async () => {
    const x = dcc.parallelize([['a', 1], ['b', 4]]) as RDD<[any, any]>;
    const y = dcc.parallelize([['a', 2]]) as RDD<[any, any]>;
    let res = await x.cogroup(y).collect();

    res = res.sort((a, b) => a[0].localeCompare(b[0]));

    expect(res).deep.equals([['a', [[1], [2]]], ['b', [[4], []]]]);
  });

  it('Test join', async () => {
    const x1 = dcc.parallelize([['a', 1], ['b', 4], ['c', 654]]) as RDD<
      [any, any]
    >;
    const y1 = dcc.parallelize([['a', 2]]) as RDD<[any, any]>;
    let res1 = (await x1.join(y1).collect()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );
    expect(res1).deep.equals([['a', [1, 2]]]);

    const x2 = dcc.parallelize([['a', 1], ['b', 4], ['c', 654]]) as RDD<
      [any, any]
    >;
    const y2 = dcc.parallelize([['a', 2], ['a', 25]]) as RDD<[any, any]>;
    let res2 = (await x2.join(y2).collect()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );
    expect(res2).deep.equals([['a', [1, 2]], ['a', [1, 25]]]);

    expect(await x2.join(dcc.emptyRDD()).collect()).deep.equals([]);
  });

  it('Test leftOuterJoin', async () => {
    const x1 = dcc.parallelize([['a', 1], ['b', 4], ['c', 654]]) as RDD<
      [any, any]
    >;
    const y1 = dcc.parallelize([['a', 2], ['z', 999]]) as RDD<[any, any]>;
    let res1 = (await x1.leftOuterJoin(y1).collect()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );

    expect(res1).deep.equals([
      ['a', [1, 2]],
      ['b', [4, null]],
      ['c', [654, null]],
    ]);

    const x2 = dcc.parallelize([['a', 1], ['b', 4], ['c', 654]]) as RDD<
      [any, any]
    >;
    const y2 = dcc.parallelize([['a', 2], ['a', 25]]) as RDD<[any, any]>;
    let res2 = (await x2.leftOuterJoin(y2).collect()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );
    expect(res2).deep.equals([
      ['a', [1, 2]],
      ['a', [1, 25]],
      ['b', [4, null]],
      ['c', [654, null]],
    ]);

    expect(
      (await x2.leftOuterJoin(dcc.emptyRDD()).collect()).sort((a, b) =>
        a[0].localeCompare(b[0]),
      ),
    ).deep.equals([['a', [1, null]], ['b', [4, null]], ['c', [654, null]]]);
  });

  it('Test rightOuterJoin', async () => {
    const x1 = dcc.parallelize([['a', 1], ['b', 4], ['c', 654]]) as RDD<
      [any, any]
    >;
    const y1 = dcc.parallelize([['a', 2], ['z', 999]]) as RDD<[any, any]>;
    let res1 = (await y1.rightOuterJoin(x1).collect()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );

    expect(res1).deep.equals([
      ['a', [2, 1]],
      ['b', [null, 4]],
      ['c', [null, 654]],
    ]);

    const x2 = dcc.parallelize([['a', 1], ['b', 4], ['c', 654]]) as RDD<
      [any, any]
    >;
    const y2 = dcc.parallelize([['a', 2], ['a', 25]]) as RDD<[any, any]>;
    let res2 = (await y2.rightOuterJoin(x2).collect()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );
    expect(res2).deep.equals([
      ['a', [2, 1]],
      ['a', [25, 1]],
      ['b', [null, 4]],
      ['c', [null, 654]],
    ]);

    expect(await x2.rightOuterJoin(dcc.emptyRDD()).collect()).deep.equals([]);
  });

  it('Test fullOuterJoin', async () => {
    const x1 = dcc.parallelize([['a', 1], ['b', 4], ['c', 654]]) as RDD<
      [any, any]
    >;
    const y1 = dcc.parallelize([['a', 2], ['z', 999]]) as RDD<[any, any]>;
    let res1 = (await x1.fullOuterJoin(y1).collect()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );

    expect(res1).deep.equals([
      ['a', [1, 2]],
      ['b', [4, null]],
      ['c', [654, null]],
      ['z', [null, 999]],
    ]);

    const x2 = dcc.parallelize([['a', 1], ['b', 4], ['c', 654]]) as RDD<
      [any, any]
    >;
    const y2 = dcc.parallelize([['a', 2], ['a', 25]]) as RDD<[any, any]>;
    let res2 = (await x2.fullOuterJoin(y2).collect()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );

    expect(res2).deep.equals([
      ['a', [1, 2]],
      ['a', [1, 25]],
      ['b', [4, null]],
      ['c', [654, null]],
    ]);

    expect(
      (await x2.fullOuterJoin(dcc.emptyRDD()).collect()).sort((a, b) =>
        a[0].localeCompare(b[0]),
      ),
    ).deep.equals([['a', [1, null]], ['b', [4, null]], ['c', [654, null]]]);

    expect(
      await dcc
        .emptyRDD()
        .fullOuterJoin(dcc.emptyRDD())
        .collect(),
    ).deep.equals([]);
  });

  it('Test chained repartition', async () => {
    const max = 100000;
    const tmp1 = dcc
      .range(0, max)
      .repartition(16)
      .repartition(8);
    const tmp2 = dcc
      .range(0, max / 4)
      .repartition(16)
      .repartition(8)
      .union(tmp1)
      .repartition(128);

    expect(await tmp2.count()).equals(max * 1.25);
    expect(await tmp2.max()).equals(max - 1);
    expect(await tmp2.min()).equals(0);
    expect(await tmp2.reduce((a, b) => a + b)).equals(4999950000 + 312487500);
  }).timeout(10000);
});
