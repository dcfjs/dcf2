export function concatArrays<T>(arr: T[][]): T[] {
  const ret = [];
  for (let subArr of arr) {
    for (let item of subArr) {
      ret.push(item);
    }
  }
  return ret;
}

export function defaultComparator(a: number, b: number): number {
  return a - b;
}
