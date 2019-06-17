import { Server, createServer } from '@dcfjs/common/server';
import {
  Client,
  createClient,
  RequestNotFoundError,
  RequestInternalServerError,
} from '@dcfjs/common/client';
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import { expect } from 'chai';

chai.use(chaiAsPromised);

describe('Server', () => {
  let server: Server;
  before(async () => {
    server = await createServer(
      {
        '/foo': () => {
          return {
            hello: 'world',
          };
        },
        '/echo': (v: any) => {
          return v;
        },
        '/error': () => {
          throw new Error('SomeError');
        },
      },
      {},
    );
  });
  after(async () => {
    await server.close();
  });
  it('Connect', async () => {
    const client = await createClient(server.endpoint);
    await client.close();
  });
  describe('TestWithConnection', () => {
    let client: Client;
    before(async () => {
      client = await createClient(server.endpoint);
    });
    after(async () => {
      await client.close();
    });
    it('Request', async () => {
      expect(await client.get('/foo')).to.deep.equals({
        hello: 'world',
      });
      expect(await client.get('/foo')).to.deep.equals({
        hello: 'world',
      });
    });
    it('Post', async () => {
      expect(await client.post('/echo', { test: 'echo' })).to.deep.equals({
        test: 'echo',
      });
    });
    it('NotFound', async () => {
      await expect(client.get('/bar')).to.be.rejectedWith(RequestNotFoundError);
    });
    it('ShouldThrow', async () => {
      await expect(client.get('/error')).to.be.rejectedWith(
        RequestInternalServerError,
      );
    });
  });
});
