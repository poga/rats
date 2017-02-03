var fs = require('fs')
var protobuf = require('protocol-buffers')
var Int64 = require('node-int64')

var messages = protobuf(fs.readFileSync('./messages.proto'))

module.exports = {encodeHeader, decodeHeader, encodeTs, decodeTs}

function encodeHeader (header) {
  var int64 = new Int64(header.baseTs.toString(16))
  var headerForEncode = Object.assign({}, header, {baseTs: int64})
  return messages.Header.encode(headerForEncode)
}

function decodeHeader (buf) {
  var header = messages.Header.decode(buf)
  header.baseTs = new Int64(header.baseTs).toNumber()
  return header
}

function encodeTs (header, idx, timestamp, value) {
  if (idx < 2) throw new Error('index must > 2')

  // calculate timestamp double-delta
  var projectedTs = header.baseTs + idx * header.baseTsDelta
  var ddTs = timestamp - projectedTs

  var dV
  // calculate value delta
  if (header.metricType === 0) {
    dV = value - idx * header.baseValueDelta
  } else if (header.metricType === 1) {
    dV = value - header.baseValue
  } else {
    throw new Error('Invalid metric type')
  }

  return messages.Record.encode({tsDoubleDelta: ddTs, valueDelta: dV})
}

function decodeTs (header, idx, buf, raw) {
  if (idx < 2) throw new Error('index must > 2')

  var record = messages.Record.decode(buf)

  if (raw) return {timestamp: record.tsDoubleDelta, value: record.valueDelta}

  var projectedTs = header.baseTs + idx * header.baseTsDelta
  var realTs = record.tsDoubleDelta + projectedTs

  var realV
  if (header.metricType === 0) {
    realV = record.valueDelta + idx * header.baseValueDelta
  } else if (header.metricType === 1) {
    realV = record.valueDelta + header.baseValue
  } else {
    throw new Error('Invalid metric type')
  }

  return {timestamp: realTs, value: realV}
}
