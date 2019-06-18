import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import { releaseAll, autoRelease } from '@dcfjs/common/autoRelease';
import { createMasterServer } from '@dcfjs/master';
import { createLocalWorker } from '@dcfjs/worker';
import { Context, createContext } from '@dcfjs/client/Context';
import { expect } from 'chai';

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

describe('MapReduce With local worker', () => {
  let dcc: Context;

  before(async () => {
    const server = await createMasterServer();
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

  it('Test range count', async () => {
    expect(await dcc.range(100).count()).to.equals(100);
    expect(await dcc.range(0, 100).count()).to.equals(100);
    expect(await dcc.range(0, 100, 4).count()).to.equals(25);
  });

  it('Test range content', async () => {
    expect(await dcc.range(100).collect()).deep.equals(
      new Array(100).fill(0).map((v, i) => i),
    );
    expect(await dcc.range(0, 100).collect()).deep.equals(
      new Array(100).fill(0).map((v, i) => i),
    );
    expect(await dcc.range(0, 100, 4).collect()).deep.equals(
      new Array(25).fill(0).map((v, i) => i * 4),
    );
  });

  it('Test union', async () => {
    expect(
      await dcc
        .range(10)
        .union(dcc.range(10, 20), dcc.range(20, 30))
        .collect(),
    ).deep.equals(await dcc.range(30).collect());
  });
});