import * as http2 from 'http2';
import streamToBuffer from './streamToBuffer';
import { EventEmitter } from 'events';
const { deserialize, serialize } = require('v8');

export type ServerHandler = (
  body: any | null,
  sess: http2.Http2Session,
) => any | Promise<any>;
export type ServerHandlerMap = { [url: string]: ServerHandler };

export interface ServerConfig {
  port?: number;
  host?: string;
  backlog?: number;
}

export interface CustomSession extends http2.ServerHttp2Session {
  id: number;
}

export class ServerError extends Error {
  code: number;
  _message?: string;
  constructor(code: number, message?: string) {
    super('ServerError');
    this.code = code;
    this._message = message;
  }
}

export class ServerBadRequestError extends ServerError {
  constructor(message: string) {
    super(http2.constants.HTTP_STATUS_BAD_REQUEST, message);
  }
}

export class Server extends EventEmitter {
  private _server: http2.Http2Server;
  private _sessions: CustomSession[] = [];

  constructor(server: http2.Http2Server) {
    super();
    this._server = server;

    server.on('session', (sess: CustomSession) => {
      sess.setTimeout(0);
      sess.id = this._sessions.length;
      this._sessions.push(sess);

      sess.on('close', () => {
        if (sess.id === this._sessions.length - 1) {
          this._sessions.pop();
        } else {
          const tmp = this._sessions.pop()!;
          tmp.id = sess.id;
          this._sessions[sess.id] = tmp;
        }
        delete sess.id;
      });
    });

    console.log('Listening at ', this.endpoint);
  }

  async close() {
    const promises: Promise<any>[] = [];

    promises.push(new Promise(resolve => this._server.close(resolve)));

    for (const sess of this._sessions.slice(0)) {
      promises.push(
        new Promise(resolve => {
          // sometimes session close event will fire later.
          if (sess.closed) {
            resolve();
          } else {
            sess.close(resolve);
          }
        }),
      );
    }

    await Promise.all(promises);
  }

  get endpoint(): string {
    let address = this._server.address();
    if (!address) {
      throw new Error('Cannot get server address.');
    }
    if (typeof address === 'object') {
      let host = address.address;
      if (host === '::' || host === '0.0.0.0') {
        host = 'localhost';
      }
      address = `${host}:${address.port}`;
    }
    return 'http://' + address;
  }
}

export async function createServer(
  handlers: ServerHandlerMap,
  config: ServerConfig = {},
): Promise<Server> {
  const server = http2.createServer();

  server.on('stream', async (stream, header) => {
    let body = null;
    const method = header[http2.constants.HTTP2_HEADER_METHOD];
    const path = header[http2.constants.HTTP2_HEADER_PATH] as string;
    if (
      method === http2.constants.HTTP2_METHOD_OPTIONS ||
      method === http2.constants.HTTP2_METHOD_HEAD
    ) {
      stream.respond({
        [http2.constants.HTTP2_HEADER_STATUS]:
          http2.constants.HTTP_STATUS_NOT_FOUND,
      });
      stream.end();
      return;
    }
    const handler = handlers[path];
    if (!handler) {
      stream.respond({
        [http2.constants.HTTP2_HEADER_STATUS]:
          http2.constants.HTTP_STATUS_NOT_FOUND,
      });
      stream.end();
      return;
    }
    try {
      if (
        method === http2.constants.HTTP2_METHOD_POST ||
        method === http2.constants.HTTP2_METHOD_PUT
      ) {
        const buf = await streamToBuffer(stream);
        body = buf.length > 0 ? deserialize(buf) : null;
      }
    } catch (e) {
      stream.respond({
        [http2.constants.HTTP2_HEADER_STATUS]:
          http2.constants.HTTP_STATUS_BAD_REQUEST,
      });
      stream.end(e.message);
    }
    try {
      let resp = await handler(body, stream.session);
      stream.end(serialize(resp));
    } catch (e) {
      console.error(e.stack);
      if (e instanceof ServerError) {
        stream.respond({
          [http2.constants.HTTP2_HEADER_STATUS]: e.code,
        });
        stream.end(e._message);
      } else {
        stream.respond({
          [http2.constants.HTTP2_HEADER_STATUS]:
            http2.constants.HTTP_STATUS_INTERNAL_SERVER_ERROR,
        });
        stream.end(e.message);
      }
    }
  });

  return new Promise((resolve, reject) => {
    server.listen(config.port, config.host, config.backlog, () =>
      resolve(new Server(server)),
    );
    server.on('error', reject);
  });
}
