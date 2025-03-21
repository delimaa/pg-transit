export function loop(callback: () => Promise<void>, delay: number, opts?: { immediate?: boolean }): () => void {
  let timer: Timer;
  let stopped = false;

  const loop = () => {
    timer = setTimeout(async () => {
      if (stopped) {
        return;
      }

      try {
        await callback();
      } finally {
        loop();
      }
    }, delay);
  };

  if (opts?.immediate) {
    callback().then(() => {
      loop();
    });
  } else {
    loop();
  }

  return () => {
    stopped = true;
    if (timer) {
      clearTimeout(timer);
    }
  };
}

export function isAsyncFunction(target: unknown): boolean {
  return typeof target === 'function' && (target as any)[Symbol.toStringTag] === 'AsyncFunction';
}

export function patchAsyncMethods<T>(target: T, config: { doBefore: (instance: T) => Promise<void> }) {
  const doBefore = config.doBefore;

  Object.getOwnPropertyNames(target).forEach((propName: any) => {
    if (propName === 'constructor') {
      return;
    }

    const prop = (target as any)[propName];

    if (!isAsyncFunction(prop)) {
      return;
    }

    (target as any)[propName] = async function (...args: any[]) {
      await doBefore(this);
      return prop.apply(this, args);
    };
  });
}
