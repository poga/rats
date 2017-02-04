var {encoder, fn} = require('..')
var tape = require('tape')
var streamify = require('stream-array')
var pump = require('pump')
var fs = require('fs')
var serialize = require('../serialize')

tape('encode', function (t) {
  pump(streamify([[0, 1], [1, 2], [2, 3]]), encoder(0), fs.createWriteStream(fn('test_encode')), function (err) {
    t.error(err)

    var buf = fs.readFileSync('test_encode.rats')

    var header = serialize.decodeHeader(buf.slice(0, 42))
    t.same(header, {
      version: 0,
      metricType: 0,
      baseTs: 0,
      baseValue: 1,
      baseTsDelta: 1,
      baseValueDelta: 1
    })
    t.same(serialize.decodeRecord(header, 2, buf.slice(42, 56)), {timestamp: 2, value: 3})

    fs.unlinkSync('test_encode.rats')
    t.end()
  })
})
