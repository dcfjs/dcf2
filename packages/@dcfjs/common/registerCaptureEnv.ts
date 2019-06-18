import { captureEnv, requireModule } from './serializeFunction';

const { addHook } = require('pirates');
const recast = require('recast');
const astTypes = require('ast-types');
const b = astTypes.builders;
const n = astTypes.namedTypes;
const parser = require('recast/parsers/babel');
const DEFAULT_EXTENSIONS = Object.freeze([
  '.js',
  '.jsx',
  '.es6',
  '.es',
  '.mjs',
]);

const KNOWN_GLOBAL: { [key: string]: boolean } = {
  Promise: true,
  Array: true,
  Buffer: true,
  require: true,
  process: true,
  global: true,
  console: true,
  Math: true,
  Error: true,
  __captureEnv: true,
  __requireModule: true,
};

function hookCode(ast: any) {
  const stack: any[] = [
    {
      knownIdentifiers: {},
      isFunction: false,
      upValues: {},
    },
  ];
  let top: any = stack[0];

  function recordRequireDeclaration(
    dec: any,
    moduleName: string,
    isVar: boolean = false,
  ) {
    if (isVar) {
      // var can be used in whole function before defined.
      delete top.upValues[dec.name];
    }
    top.knownIdentifiers[dec.name] = moduleName;
  }

  function getIdentifierFromDefinition(dec: any, isVar: boolean = false) {
    if (n.Identifier.check(dec)) {
      if (isVar) {
        // var can be used in whole function before defined.
        delete top.upValues[dec.name];
      }
      top.knownIdentifiers[dec.name] = top.knownIdentifiers[dec.name] || true;
    }
    if (n.ObjectPattern.check(dec)) {
      for (const property of dec.properties) {
        getIdentifierFromDefinition(property.value, isVar);
      }
    }
    if (n.ArrayPattern.check(dec)) {
      for (const elements of dec.elements) {
        getIdentifierFromDefinition(elements, isVar);
      }
    }
    if (n.AssignmentPattern.check(dec)) {
      getIdentifierFromDefinition(dec.left, isVar);
    }
    if (n.RestElement.check(dec)) {
      getIdentifierFromDefinition(dec.argument, isVar);
    }
  }

  function registerUpValue(name: string) {
    let moduleName = true;
    for (let i = stack.length - 1; i >= 0; i--) {
      const curr = stack[i];
      if (curr.knownIdentifiers[name]) {
        moduleName = curr.knownIdentifiers[name];
        break;
      }
    }

    for (let i = stack.length - 1; i >= 0; i--) {
      const curr = stack[i];
      if (curr.knownIdentifiers[name]) {
        break;
      }
      if (curr.isFunction) {
        curr.upValues[name] = moduleName;
        if (typeof moduleName === 'string') {
          return;
        }
      }
    }
  }

  function pushStack(isFunction: boolean) {
    let knownIdentifiers: any = {};

    if (!isFunction) {
      // copy every identifier.
      Object.assign(knownIdentifiers, top.knownIdentifiers);
    }
    stack.push(
      (top = {
        knownIdentifiers,
        isFunction,
        upValues: isFunction ? {} : top.isFunction,
      }),
    );
  }

  function popStack() {
    const ret = stack.pop();
    top = stack[stack.length - 1] || null;
    return ret;
  }

  function generateEnvProperty(name: string, value: string | true) {
    if (value === true) {
      return b.property('init', b.identifier(name), b.identifier(name));
    }
    const isRelative = /^\.\.?/.test(value);
    if (!isRelative) {
      return b.property(
        'init',
        b.identifier(name),
        b.callExpression(b.identifier('__requireModule'), [b.literal(value)]),
      );
    }

    return b.property(
      'init',
      b.identifier(name),
      b.callExpression(b.identifier('__requireModule'), [
        b.callExpression(
          b.memberExpression(b.identifier('require'), b.identifier('resolve')),
          [b.literal(value)],
        ),
      ]),
    );
  }

  recast.visit(ast, {
    visitIdentifier(path: any) {
      this.traverse(path);

      const self = path.value;
      const parent = path.parentPath.value;
      if (n.Property.check(parent) || n.ObjectProperty.check(parent)) {
        if (self === parent.key) {
          return;
        }
      }
      if (n.MemberExpression.check(parent)) {
        if (self === parent.property) {
          return;
        }
      }
      if (!KNOWN_GLOBAL[self.name]) {
        registerUpValue(self.name);
      }
    },
    visitVariableDeclaration(path: any) {
      const isVar = path.value.kind === 'var';
      for (const item of path.value.declarations) {
        if (
          n.CallExpression.check(item.init) &&
          n.Identifier.check(item.init.callee) &&
          item.init.callee.name === 'require' &&
          n.Literal.check(item.init.arguments[0])
        ) {
          // This is a require declaration
          recordRequireDeclaration(
            item.id,
            item.init.arguments[0].value,
            isVar,
          );
        } else {
          getIdentifierFromDefinition(item.id, isVar);
        }
      }
      this.traverse(path);
    },
    visitBlock(path: any) {
      pushStack(false);
      this.traverse(path);
      popStack();
    },
    visitCatchClause(path: any) {
      pushStack(false);
      getIdentifierFromDefinition(path.value.param);
      this.traverse(path);
      popStack();
    },
    visitFunctionDeclaration(path: any) {
      getIdentifierFromDefinition(path.value.id, true);
      pushStack(true);
      getIdentifierFromDefinition(path.value.id, true);
      for (const item of path.value.params) {
        if (n.AssignmentExpression.check(item)) {
          getIdentifierFromDefinition(item.left);
        } else {
          getIdentifierFromDefinition(item);
        }
      }
      this.traverse(path);
      const { upValues } = popStack();
      path.insertAfter(
        b.expressionStatement(
          b.callExpression(b.identifier('__captureEnv'), [
            path.value.id,
            b.objectExpression(
              Object.keys(upValues).map(v =>
                generateEnvProperty(v, upValues[v]),
              ),
            ),
          ]),
        ),
      );
    },
    visitArrowFunctionExpression(path: any) {
      pushStack(true);
      for (const item of path.value.params) {
        if (n.AssignmentExpression.check(item)) {
          getIdentifierFromDefinition(item.left);
        } else {
          getIdentifierFromDefinition(item);
        }
      }
      this.traverse(path);
      const { upValues } = popStack();

      if (Object.keys(upValues).length > 0) {
        path.replace(
          b.callExpression(b.identifier('__captureEnv'), [
            path.value,
            b.objectExpression(
              Object.keys(upValues).map(v =>
                generateEnvProperty(v, upValues[v]),
              ),
            ),
          ]),
        );
      }
    },
    visitFunctionExpression(path: any) {
      pushStack(true);
      for (const item of path.value.params) {
        getIdentifierFromDefinition(item);
      }
      this.traverse(path);
      const { upValues } = popStack();

      if (Object.keys(upValues).length > 0) {
        path.replace(
          b.callExpression(b.identifier('__captureEnv'), [
            path.value,
            b.objectExpression(
              Object.keys(upValues).map(v =>
                generateEnvProperty(v, upValues[v]),
              ),
            ),
          ]),
        );
      }
    },
  });
}

export function transformCode(code: string, filename: string) {
  if (/@noCaptureEnv/.test(code)) {
    return code;
  }

  const ast = recast.parse(code, { parser });
  hookCode(ast);

  return recast.print(ast).code;
}

addHook(transformCode, {
  exts: DEFAULT_EXTENSIONS,
});

(global as any).__captureEnv = captureEnv;
(global as any).__requireModule = requireModule;
