import {
  serializeFunction,
  deserializeFunction,
} from '@dcfjs/common/serializeFunction';
import { expect } from 'chai';
import { cpus } from 'os';

describe('SerializeFunction', () => {
  it('SimpleFunction', () => {
    const f = () => 1;
    const f1 = deserializeFunction(serializeFunction(f));
    expect(f1()).to.equal(1);
  });

  it('ManuallyEnv', () => {
    const a = 1;
    const f = () => a;
    const f1 = deserializeFunction(serializeFunction(f, { a }));
    expect(f1()).to.equal(1);
  });

  it('AutoEnv', () => {
    const a = 1;
    const f = () => a;
    const f1 = deserializeFunction(serializeFunction(f));
    expect(f1()).to.equal(f1());
  });

  it('AutoEnvFunction', () => {
    const a = 1;
    function f() {
      return a;
    }
    const f1 = deserializeFunction(serializeFunction(f));
    expect(f1()).to.equal(f1());
  });

  it(`AutoEnvWithUpFunction`, () => {
    const a = 1;
    function f() {
      return a;
    }
    function g() {
      return f();
    }
    const g1 = deserializeFunction(serializeFunction(g));
    expect(g1()).to.equal(g1());
  });

  it('TypescriptImport', () => {
    const f = () => cpus()[0].model;
    const f1 = deserializeFunction(serializeFunction(f));
    expect(f1()).to.equal(f());
  });

  it('RelativeImport', () => {
    // return a function from required module, should be same instance on same process.
    const f = () => serializeFunction;
    const f1 = deserializeFunction(serializeFunction(f));
    expect(f1()).to.equal(f());
  });

  it('MutateUpValue', () => {
    let a = 0;
    const f = () => (a += 1);
    const f1 = deserializeFunction(serializeFunction(f));
    expect(f1).to.throw();
  });

  it('localRequire', () => {
    const f = () => require('os').cpus()[0].model;
    const f1 = deserializeFunction(serializeFunction(f));
    expect(f1()).to.equal(f());
  });

  it('Override global name', () => {
    const Buffer = 1;
    const f = () => Buffer;
    const f1 = deserializeFunction(serializeFunction(f));
    expect(f1()).to.equal(f());
  });

  // This test case is not expected, because Buffer was changed(undefined -> 1) after function declared.
  // it('Override global name by var', () => {
  //   const f = () => Buffer;
  //   var Buffer = 1;
  //   const f1 = deserializeFunction(serializeFunction(f));
  //   expect(f1()).to.equal(f());
  // });
});
