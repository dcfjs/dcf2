import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import { releaseAll, autoRelease } from '@dcfjs/common/autoRelease';
import { createMasterServer } from '@dcfjs/master';
import { createLocalWorker } from '@dcfjs/worker';
import { Context, createContext } from '@dcfjs/client/Context';
import { expect } from 'chai';
import { RDD } from '@dcfjs/client';

chai.use(chaiAsPromised);

describe('MapReduce With local worker', () => {
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

  it('Test sort', async () => {
    const tmp = [1, 5, 2, 4, 6, 9, 0, 8, 7, 3];
    expect(
      await dcc
        .parallelize(tmp)
        .sort()
        .collect(),
    ).deep.equals([...tmp].sort());
  });

  it('Test stable sort', async () => {
    const tmp = [[1, 1], [2, 1], [1, 2], [2, 2], [1, 3], [2, 3]];
    expect(
      await dcc
        .parallelize(tmp)
        .sortBy(v => v[0])
        .collect(),
    ).deep.equals([...tmp].sort((a, b) => a[0] - b[0]));
  });
});
