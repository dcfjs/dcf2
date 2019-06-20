export function concatArrays<T>(arr: T[][]): T[] {
  const ret = [];
  for (let subArr of arr) {
    for (let item of subArr) {
      ret.push(item);
    }
  }
  return ret;
}

export function defaultComparator(a: any, b: any): any {
  return a - b;
}
