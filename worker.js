const { Readable, Writable } = require('stream');

const streamFactoryModulePath = process.argv[2];
const streamFactory = require(streamFactoryModulePath);

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
process.on('message', hander);

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
    process.send(chunk);
    cb();
  },
  destroy() {
    process.off('message', hander);
  },
  objectMode: true,
  autoDestroy: true
})


streamFactory(workerInputStream).pipe(workerOutputStream);