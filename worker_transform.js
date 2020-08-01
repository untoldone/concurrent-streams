const { Transform } = require('stream');
const { fork } = require('child_process');

class StreamWorkerPool {
  constructor(concurency, modulePath, notifyCb) {
    this.idleWorkers = [];
    this.allWorkers = [];
    this.pendingItems = [];
    this.notifyCb = notifyCb;
    this.emptyQueueCb = null;

    for(var i = 0; i < concurency; i++) {
      var worker = fork('./worker.js', [modulePath]);
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
        worker.send(null);
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

      worker.send(item);
      cb();
    }

    if (this.pendingItems.length === 0 &&
        this.idleWorkers.length === this.allWorkers.length &&
        this.emptyQueueCb != null) {
      this.emptyQueueCb();
    }
  }
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