import * as stream from 'stream';

export default function streamToBuffer(
  stream: stream.Readable,
): Promise<Buffer> {
  let buffers: Buffer[] = [];
  stream.on('data', chunk => buffers.push(chunk));
  return new Promise((resolve, reject) => {
    stream.on('end', () => {
      resolve(Buffer.concat(buffers));
    });
    stream.on('error', reject);
  });
}
