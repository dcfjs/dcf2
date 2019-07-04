import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import { releaseAll, autoRelease } from '@dcfjs/common/autoRelease';
import { createMasterServer } from '@dcfjs/master';
import { createLocalWorker } from '@dcfjs/worker';
import { Context, createContext } from '@dcfjs/client/Context';
import { expect } from 'chai';
import { RDD } from '@dcfjs/client';

chai.use(chaiAsPromised);

describe('Recovery from script error.', () => {
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

  it('Test dcf function error', async () => {
    expect(
      dcc
        .range(0, 100, 4)
        .map(a => {
          throw new Error('some error');
        })
        .count(),
    ).to.be.rejectedWith();
  });
});
