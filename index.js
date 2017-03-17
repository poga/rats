const fs = require('fs')
const pb = require('protocol-buffers')
const path = require('path')
const uint64be = require('uint64be')

const messages = pb(fs.readFileSync(path.join(__dirname, 'messages.proto')))

const VERSION = 1
const HEADER_SIZE = 14
const INDEX_ITEM_SIZE = 15

const DEFAULT_MAX_SEGMENT_SIZE = 5 * 1024 * 1024 // max segment size in bytes

module.exports = RATS

function RATS (path, opts) {
  opts = Object.assign({}, {maxSegmentSize: DEFAULT_MAX_SEGMENT_SIZE}, opts)
  this.path = path
  this.maxSegmentSize = opts.maxSegmentSize

  if (!fs.statSync(this.path).isDirectory()) throw new Error('path must be a directory')

  var maxOffset = 0
  var files = fs.readdirSync(this.path)
  if (files.length === 0) {
    this.currentOffset = 0
    this.currentSegmentOffset = 0
    this.currentSegmentSize = 0
  } else {
    files.forEach(function (file) {
      if (file.endsWith('.index')) {
        var offset = file.match(/^(\d+)\.index/)[1]
        if (offset > maxOffset) maxOffset = offset
      }
    })
    this.currentSegmentOffset = maxOffset
    this.currentOffset = this.currentSegmentOffset + (fs.statSync(this.currentIndex()).size - HEADER_SIZE) / INDEX_ITEM_SIZE
    this.currentSegmentSize = fs.statSync(this.currentLog()).size
  }
}

RATS.prototype.append = function (obj, time, cb) {
  if (!cb) {
    cb = time // time is optional
    time = Math.round(Date.now() / 1000)
  }
  var data = new Buffer(JSON.stringify(obj) + '\n')
  var self = this

  if (this.currentOffset === 0) {
    appendHeader(time)
  }

  if (this.currentSegmentSize >= this.maxSegmentSize) {
    this.currentSegmentOffset = this.currentOffset
    this.currentSegmentSize = 0
    appendHeader(time)
  }

  appendLog(data, time)

  function appendLog (data, time) {
    fs.appendFile(self.currentLog(), data, function (err) {
      if (err) return cb(err)

      appendIndex()
    })
  }

  function appendIndex () {
    var index = {offset: self.currentOffset, position: self.currentSegmentSize, size: data.length}
    fs.appendFile(self.currentIndex(), messages.Index.encode(index), function (err) {
      if (err) return cb(err)
      self.currentSegmentSize += data.length
      self.currentOffset++

      cb()
    })
  }

  function appendHeader (baseTimestamp) {
    var header = {version: VERSION, baseTimestamp: uint64be.encode(baseTimestamp)}
    fs.appendFileSync(self.currentIndex(), messages.Header.encode(header))
  }
}

RATS.prototype.currentIndex = function () {
  return path.join(this.path, `${this.currentSegmentOffset}.index`)
}

RATS.prototype.currentLog = function () {
  return path.join(this.path, `${this.currentSegmentOffset}.log`)
}
