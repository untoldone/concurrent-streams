const workerTransform = require('./worker_transform');
const path = require('path');
const { Readable, Transform } = require('stream');

var modulePath = path.resolve('./first.js');

var i = 0;
var r = new Readable({
  read() {
    if(i <= 100) {
      this.push(i);
      i++;
    } else {
      this.push(null);
    }
  },
  objectMode: true
});

var t = new Transform({
  transform(chunk, _, cb) {
    cb(null, chunk.toString() + "\n");
  },
  objectMode: true
})

r.pipe(workerTransform(13, modulePath)).pipe(t).pipe(process.stdout);
