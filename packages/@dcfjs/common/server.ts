import * as http2 from 'http2';
import streamToBuffer from './streamToBuffer';
import { EventEmitter } from 'events';
const { deserialize, serialize } = require('v8');

export type ServerHandler = (
  body: any | null,
  sess: http2.Http2Session,
  stream: http2.ServerHttp2Stream,
) => any | Promise<any>;
export type ServerHandlerMap = { [url: string]: ServerHandler };

type ServerStorageConfig = {
  name: string; // name of registered storage.
  module: string; // name of a module provides a factory.
  options?: any;
};

export interface ServerConfig {
  port?: number;
  host?: string;
  backlog?: number;
  storages?: ServerStorageConfig[];
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

    server.on('close', (...args) => {
      this.emit('close', ...args);
    });
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
    const handler = handlers[path];
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
      return;
    }
    try {
      let resp = await handler(body, stream.session, stream);
      stream.end(serialize(resp));
    } catch (e) {
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
        stream.end(e.stack + e.message);
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

export async function pushStream(
  stream: http2.ServerHttp2Stream,
  path: string,
): Promise<http2.ServerHttp2Stream> {
  return new Promise((resolve, reject) => {
    stream.pushStream(
      {
        [http2.constants.HTTP2_HEADER_PATH]: path,
      },
      (err, pushStream) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(pushStream);
      },
    );
  });
}
