var tape = require('tape')
var rats = require('.')
var Int64 = require('node-int64')

tape('header', function (t) {
  var buf = rats.encodeHeader({
    version: 0,
    metricType: 0,
    baseTs: new Int64((1234).toString(16)),
    baseValue: 1,
    baseTsDelta: 3,
    baseValueDelta: 2
  })
  var header = rats.decodeHeader(buf)
  t.same(header, {
    version: 0,
    metricType: 0,
    baseTs: new Int64((1234).toString(16)),
    baseValue: 1,
    baseTsDelta: 3,
    baseValueDelta: 2
  })
  t.end()
})

tape('invalid header', function (t) {
  var buf = new Buffer('Hello')

  t.throws(function () { rats.decodeHeader(buf) })
  t.end()
})

tape('record, metricType = 0', function (t) {
  var now = Math.floor(Date.now() / 1000.0) // seconds
  var header = {
    version: 0,
    metricType: 0,
    baseTs: new Int64((now).toString(16)),
    baseValue: 10,
    baseTsDelta: 1,
    baseValueDelta: 2
  }

  var buf = rats.encodeTs(header, 2, now + 2, 4)
  var raw = rats.decodeTs(header, 2, buf, true)
  t.same(raw, {timestamp: 0, value: 0})
  var rec = rats.decodeTs(header, 2, buf)
  t.same(rec, {timestamp: now + 2, value: 4})
  t.end()
})

tape('record, metricType = 1', function (t) {
  var now = Math.floor(Date.now() / 1000.0) // seconds
  var header = {
    version: 0,
    metricType: 1,
    baseTs: new Int64((now).toString(16)),
    baseValue: 10,
    baseTsDelta: 1,
    baseValueDelta: 2
  }

  var buf = rats.encodeTs(header, 2, now + 2, 4)
  var raw = rats.decodeTs(header, 2, buf, true)
  t.same(raw, {timestamp: 0, value: -6})
  var rec = rats.decodeTs(header, 2, buf)
  t.same(rec, {timestamp: now + 2, value: 4})
  t.end()
})
