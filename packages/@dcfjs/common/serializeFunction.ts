export interface SerializedFunction<T> {
  __type: 'function';
  source: string;
  args: string[];
  values: any[];
}

export type FunctionEnv = { [key: string]: any };

class RequireModule {
  moduleName: string;
  constructor(moduleName: string) {
    this.moduleName = moduleName;
  }
}

function serializeValue(v: any) {
  if (typeof v === 'function') {
    return serializeFunction(v);
  }
  if (v && typeof v === 'object') {
    if (v.constructor === RequireModule) {
      return {
        __type: 'require',
        moduleName: v.moduleName,
      };
    }
    if (v.constructor !== Object && !Array.isArray(v)) {
      throw new Error(`Cannot pass a ${v.constructor.name} object`);
    }
    if (v.__type) {
      // handle a native object with __type field. This is a rare case.
      return {
        __type: 'object',
        value: v,
      };
    }
    return v;
  }
  return v;
}

function deepFreeze(o: any) {
  Object.freeze(o);
  for (const propKey in o) {
    const prop = o[propKey];
    if (
      !o.hasOwnProperty(propKey) ||
      !(typeof prop === 'object') ||
      Object.isFrozen(prop)
    ) {
      continue;
    }
    deepFreeze(prop);
  }
  return o;
}

function deserializeValue(v: any) {
  if (v && typeof v === 'object') {
    if (Array.isArray(v)) {
      return deepFreeze(v);
    }
    if (v.__type) {
      switch (v.__type) {
        case 'object':
          return deepFreeze(v.value);
        case 'function':
          return deserializeFunction(v, true);
        case 'require':
          return require(v.moduleName);
      }
    }
    return deepFreeze(v);
  }
  return v;
}

const requireModuleCache: { [key: string]: RequireModule } = {};

export function requireModule(v: string) {
  return (requireModuleCache[v] =
    requireModuleCache[v] || new RequireModule(v));
}

export function serializeFunction<T extends (...args: any[]) => any>(
  f: T & {
    __env?: FunctionEnv;
  },
  env?: FunctionEnv,
): SerializedFunction<T> {
  env = env || f.__env;
  const args: string[] = [];
  const values: any[] = [];
  const functions: any[] = [];

  if (env) {
    for (const key of Object.keys(env)) {
      args.push(key);
      values.push(serializeValue(env[key]));
    }
  }

  return {
    __type: 'function',
    source: f.toString(),
    args,
    values,
    functions,
  } as SerializedFunction<T>;
}

function wrap<T extends (...args: any[]) => any>(f: T) {
  return function(...args: any[]) {
    try {
      return f(...args);
    } catch (e) {
      throw new Error(`In function ${f.toString()}\n${e.stack}`);
    }
  };
}

export function deserializeFunction<T extends (...args: any[]) => any>(
  f: SerializedFunction<T>,
  noWrap?: boolean,
): T {
  let ret;
  if (noWrap) {
    ret = new Function(
      'require',
      '__args',
      `const [${f.args.join(',')}] = __args;
  return ${f.source}`,
    )(require, f.values.map(v => deserializeValue(v)));
  } else {
    ret = new Function(
      'require',
      '__wrap',
      '__args',
      `const [${f.args.join(',')}] = __args;
return __wrap(${f.source})`,
    )(require, wrap, f.values.map(v => deserializeValue(v)));
  }
  const __env: FunctionEnv = {};
  for (let i = 0; i < f.args.length; i++) {
    __env[f.args[i]] = f.values[i];
  }
  ret.__env = __env;
  return ret;
}

export function captureEnv<T extends (...args: any[]) => any>(
  f: T & {
    __env?: FunctionEnv;
  },
  env: FunctionEnv,
) {
  f.__env = env;
  return f;
}
