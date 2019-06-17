const seq = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

// 采用第一位作为长度表示，确保可以符合字典序
// 00, 01, ... 0Z, 100
const first = seq[0];
const last = seq[seq.length - 1];

const seqMap: { [key: string]: string } = {};
for (let i = 0; i < seq.length - 1; i++) {
  seqMap[seq[i]] = seq[i + 1];
}

export function next(a: string): string {
  if (!a) {
    return '0';
  }
  const ch0 = a[a.length - 1];
  const tail = a.substr(0, a.length - 1);

  if (ch0 === last) {
    return next(tail) + first;
  }
  return tail + seqMap[ch0];
}

export function iterator(from: string = '0'): () => string {
  let tmp: string = from;
  return () => {
    const ret = tmp;
    tmp = next(tmp);
    return ret;
  };
}
