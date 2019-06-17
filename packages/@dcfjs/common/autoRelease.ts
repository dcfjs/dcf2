export type CleaupFunction = () => any | Promise<any>;
const release: CleaupFunction[] = [];

export function autoRelease(func: CleaupFunction) {
  release.push(func);
}

export async function releaseAll() {
  while (release.length > 0) {
    await release.pop()!();
  }
}

export function waitForExitSignal(log = true) {
  return new Promise(resolve => {
    async function stopServer() {
      process.removeListener('SIGINT', stopServer);
      process.removeListener('SIGTERM', stopServer);
      process.removeListener('SIGHUP', stopServer);

      log && console.log('Shutting down...');
      // stop timer & wakeup listener
      await releaseAll();
      log && console.log('Bye.');
      resolve();
    }
    process.on('SIGINT', stopServer);
    process.on('SIGTERM', stopServer);
    process.on('SIGHUP', stopServer);

    // Waiting for signal:
    log && console.log('Ready.');
  });
}
