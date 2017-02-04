const serialize = require('./serialize')
const through2 = require('through2')

const VERSION = 0

module.exports = {fn, encoder}

function fn (name, labels) {
  var fn = [name]
  if (labels) fn.push(labels.keys.sort().map(k => labels[k]).join('.'))
  fn.push('rats')
  console.log(fn.join('.'))
  return fn.join('.')
}

function encoder (type) {
  var header = {
    version: VERSION,
    metricType: type
  }
  var idx = 0

  return through2.obj(function (chunk, enc, cb) {
    if (idx === 0) {
      header.baseTs = chunk[0]
      header.baseValue = chunk[1]
    } else if (idx === 1) {
      header.baseTsDelta = chunk[0] - header.baseTs
      header.baseValueDelta = chunk[1] - header.baseValue
    }

    if (idx === 1) {
      this.push(serialize.encodeHeader(header))
    } else if (idx > 1) {
      this.push(serialize.encodeTs(header, idx, chunk[0], chunk[1]))
    }

    idx += 1
    cb()
  })
}

