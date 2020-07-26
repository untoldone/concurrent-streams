const { Readable, Writable, Transform } = require('stream');
const { Worker, isMainThread, parentPort, workerData, threadId } = require('worker_threads');

class StreamWorkerPool {
  constructor(concurency, modulePath, notifyCb) {
    this.idleWorkers = [];
    this.allWorkers = [];
    this.pendingItems = [];
    this.notifyCb = notifyCb;
    this.emptyQueueCb = null;

    for(var i = 0; i < concurency; i++) {
      var worker = new Worker(__filename, { workerData: modulePath });

      worker.on('error', function (err) {
        console.error(err);
      })

      this.idleWorkers.push(worker);
      this.allWorkers.push(worker);
    }
  }

  issueWork(item, cb) {
    this.pendingItems.push([item, cb]);
    this._processOutstandingItems();
  }

  shutdown(completed) {
    this.emptyQueueCb = () => {
      this.allWorkers.forEach(function(worker) {
        worker.postMessage(null);
      });
      completed();
    }
  }

  _processOutstandingItems() {
    while(this.pendingItems.length > 0 && this.idleWorkers.length > 0) {
      var worker = this.idleWorkers.pop();
      var [item, cb] = this.pendingItems.pop();

      //worker.setMaxListeners(1000);
      worker.once('message', (completedItem) => {
        this.notifyCb(completedItem);
        this.idleWorkers.push(worker);
        this._processOutstandingItems();
      });

      worker.postMessage(item);
      cb();
    }

    if (this.pendingItems.length === 0 &&
        this.idleWorkers.length === this.allWorkers.length &&
        this.emptyQueueCb != null) {
      this.emptyQueueCb();
    }
  }
}

if (!isMainThread) {
  const streamFactory = require(workerData);

  var pendingRead = false,
      pendingInput = null,
      workerInputStream,
      workerOutputStream;


  var hander = (message) => {
    if(pendingRead) {
      workerInputStream.push(message);
      pendingRead = false;
    } else {
      pendingInput = message;
    }
  }
  parentPort.on('message', hander);

  workerInputStream = new Readable({
    read(size) {
      if(pendingInput !== null) {
        this.push(pendingInput);
        pendingInput = null;
      } else {
        pendingRead = true;
      }
    },
    objectMode: true
  });

  workerOutputStream = new Writable({
    write(chunk, _, cb) {
      parentPort.postMessage(chunk);
      cb();
    },
    destroy() {
      parentPort.off('message', hander);
    },
    objectMode: true,
    autoDestroy: true
  })


  streamFactory(workerInputStream).pipe(workerOutputStream);
}

/**
 * Creates a Transform that executes concurrently using workers
 * 
 * @param {number} concurency The number of workers.
 * @param {string} modulePath Absolute path to transform module's js code. Export must be a function that takes a Readable stream and returns a Writable stream.
 */
module.exports = function factory(concurency, modulePath) {
  var pool;
  var transform = new Transform({
    objectMode: true,
    transform(chunk, _, cb) {
      pool.issueWork(chunk, cb);
    },
    final(cb) {
      pool.shutdown(cb);
    }
  });

  pool = new StreamWorkerPool(concurency, modulePath, transform.push.bind(transform));

  return transform;
}