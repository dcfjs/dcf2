export type CleanupFunction = () => any | Promise<any>;
const release: CleanupFunction[] = [];

export function autoRelease(func: CleanupFunction) {
  release.push(func);
}

export async function releaseAll() {
  while (release.length > 0) {
    await release.pop()!();
  }
}

export function waitForExitSignal(debug: (...args: any) => void = console.log) {
  return new Promise(resolve => {
    async function stopServer() {
      process.removeListener('SIGINT', stopServer);
      process.removeListener('SIGTERM', stopServer);
      process.removeListener('SIGHUP', stopServer);

      debug('Shutting down...');
      // stop timer & wakeup listener
      await releaseAll();
      debug('Bye.');
      resolve();
    }
    process.on('SIGINT', stopServer);
    process.on('SIGTERM', stopServer);
    process.on('SIGHUP', stopServer);

    // Waiting for signal:
    debug('Ready.');
  });
}
