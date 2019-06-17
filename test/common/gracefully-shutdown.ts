import chai = require('chai');
import chaiAsPromised = require('chai-as-promised');
import { createServer } from '@dcfjs/common/server';
import { createClient } from '@dcfjs/common/client';
import { connect, Socket } from 'net';
import { expect } from 'chai';

chai.use(chaiAsPromised);

function wait(time: number) {
  return new Promise(resolve => setTimeout(resolve, time));
}

describe('GracefullyShutdown', () => {
  it('Test with health client', async () => {
    const server = await createServer({}, {});

    const client = await createClient(server.endpoint);
    await server.close();
    await client.close();
  });

  it('Test with requesting', async () => {
    const server = await createServer(
      {
        '/foo': async () => {
          await wait(500);
          return {
            hello: 'world',
          };
        },
      },
      {},
    );

    const client = await createClient(server.endpoint);
    const request = client.get('/foo');

    await wait(200);

    await server.close();
    expect(await request).to.deep.equal({
      hello: 'world',
    });
    await client.close();
  });

  it('Test with unhealth client', async () => {
    const server = await createServer(
      {},
      {
        port: 8001,
      },
    );

    const client = await new Promise<Socket>(resolve => {
      let socket: Socket;
      socket = connect(
        8001,
        'localhost',
        () => resolve(socket),
      );
    });
    await server.close();
    client.destroy();
  });
});
