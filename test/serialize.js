var tape = require('tape')
var serialize = require('../serialize')

tape('header', function (t) {
  var buf = serialize.encodeHeader({
    version: 0,
    metricType: 0,
    baseTs: 1234,
    baseValue: 1,
    baseTsDelta: 3,
    baseValueDelta: 2
  })
  var header = serialize.decodeHeader(buf)
  t.same(header, {
    version: 0,
    metricType: 0,
    baseTs: 1234,
    baseValue: 1,
    baseTsDelta: 3,
    baseValueDelta: 2
  })
  t.end()
})

tape('invalid header', function (t) {
  var buf = new Buffer('Hello')

  t.throws(function () { serialize.decodeHeader(buf) })
  t.end()
})

tape('record, metricType = 0', function (t) {
  var now = Math.floor(Date.now() / 1000.0) // seconds
  var header = {
    version: 0,
    metricType: 0,
    baseTs: now,
    baseValue: 10,
    baseTsDelta: 1,
    baseValueDelta: 2
  }

  var buf = serialize.encodeRecord(header, 2, now + 2, 4)
  var raw = serialize.decodeRecord(header, 2, buf, true)
  t.same(raw, [0, 0])
  var rec = serialize.decodeRecord(header, 2, buf)
  t.same(rec, [now + 2, 4])
  t.end()
})

tape('record, metricType = 1', function (t) {
  var now = Math.floor(Date.now() / 1000.0) // seconds
  var header = {
    version: 0,
    metricType: 1,
    baseTs: now,
    baseValue: 10,
    baseTsDelta: 1,
    baseValueDelta: 2
  }

  var buf = serialize.encodeRecord(header, 2, now + 2, 4)
  var raw = serialize.decodeRecord(header, 2, buf, true)
  t.same(raw, [0, -6])
  var rec = serialize.decodeRecord(header, 2, buf)
  t.same(rec, [now + 2, 4])
  t.end()
})
