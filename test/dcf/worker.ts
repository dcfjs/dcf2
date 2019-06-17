import { Client, createClient } from '@dcfjs/common/client';
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import { expect } from 'chai';
import { releaseAll, autoRelease } from '@dcfjs/common/autoRelease';
import { createMasterServer } from '../../packages/@dcfjs/master';
import { createLocalWorker } from '../../packages/@dcfjs/worker';
import { serializeFunction } from '@dcfjs/common/serializeFunction';
import { WorkDispatcher } from '../../packages/@dcfjs/master/workerManager';
import { AssertionError } from 'assert';

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

describe('DCF With local worker', () => {
  let client: Client;

  before(async () => {
    const server = await createMasterServer();
    client = await createClient(server.endpoint);
    autoRelease(() => server.close());

    for (let i = 0; i < 4; i++) {
      autoRelease(await createLocalWorker(server.endpoint));
    }
  });

  after(async () => {
    await releaseAll();
  });

  it('Prime test', async () => {
    const primeCount = await client.post('/exec', {
      func: serializeFunction(async (dispatchWork: WorkDispatcher) => {
        const counts = await Promise.all(
          new Array(100).fill(0).map((v, i) =>
            dispatchWork(() => {
              const from = i * 10000;
              const to = (i + 1) * 10000;

              let ret = 0;

              for (let i = from; i < to; i++) {
                if (isPrime(i)) {
                  ret++;
                }
              }
              return ret;
            }),
          ),
        );
        return counts.reduce((a, b) => a + b);
      }),
    });
    console.log('Prime count in 0-999999 is(parallel): ', primeCount);
    let ret = 0;
    for (let i = 0; i < 999999; i++) {
      if (isPrime(i)) {
        ret++;
      }
    }
    console.log('Prime count in 0-999999 is(local): ', ret);
    expect(primeCount).equals(ret);
  });
});
