const { Transform } = require('stream');

module.exports = function(inputStream) {
  var t = new Transform({
    transform(chunk, _, cb) {
      setTimeout(() => {
        cb(null, chunk * 5);
      }, 1000);
    },
    objectMode: true
  });

  return inputStream.pipe(t);
}