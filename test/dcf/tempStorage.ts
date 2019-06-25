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

  it('Test cached pieces', async () => {
    const primes1 = dcc.range(0, 100000).filter(isPrime);

    const primes2 = primes1.persist('disk');

    expect(await primes1.count()).equals(await primes2.count());
    expect(await primes1.collect()).deep.equals(await primes2.collect());
  });
});
