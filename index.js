const fs = require('fs')
const pb = require('protocol-buffers')
const path = require('path')

const messages = pb(fs.readFileSync(path.join(__dirname, 'messages.proto')))

const INDEX_ITEM_SIZE = 15

module.exports = RATS

function RATS (path, opts) {
  opts = opts || {}
  this.path = path

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
    this.currentOffset = this.currentSegmentOffset + (fs.statSync(this.currentIndex()).size) / INDEX_ITEM_SIZE
    this.currentSegmentSize = fs.statSync(this.currentLog).size
  }

  this.maxSegmentSize = opts.maxSegmentSize || 200 * 1024 * 1024 // max segment size in bytes
}

RATS.prototype.append = function (obj, time, cb) {
  if (!cb) {
    cb = time // time is optional
    time = Date.now()
  }
  var data = new Buffer(JSON.stringify(Object.assign({}, obj, {time: time})))
  var self = this

  if (this.currentSegmentSize >= this.maxSegmentSize) {
    this.currentSegmentOffset = this.currentOffset
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
      this.currentSegmentSize += data.length

      cb()
    })
  }
}

RATS.prototype.currentIndex = function () {
  return path.join(this.path, `${this.currentSegmentOffset}.index`)
}

RATS.prototype.currentLog = function () {
  return path.join(this.path, `${this.currentSegmentOffset}.log`)
}
