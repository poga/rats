const fs = require('fs')
const pb = require('protocol-buffers')
const path = require('path')
const uint64be = require('uint64be')
const async = require('async')
const hl = require('highland')
const ndjson = require('ndjson')
const through2 = require('through2')

const messages = pb(fs.readFileSync(path.join(__dirname, 'messages.proto')))

const VERSION = 1
const HEADER_SIZE = 14
const INDEX_ITEM_SIZE = 15

const DEFAULT_MAX_SEGMENT_SIZE = 5 * 1024 * 1024 // max segment size in bytes

module.exports = RATS
module.exports.VERSION = VERSION
module.exports.HEADER_SIZE = HEADER_SIZE
module.exports.INDEX_ITEM_SIZE = INDEX_ITEM_SIZE

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
      if (isIndexFile(file)) {
        var offset = getSegmentOffset(file)
        if (offset > maxOffset) maxOffset = offset
      }
    })
    this.currentSegmentOffset = maxOffset
    this.currentOffset = this.currentSegmentOffset + segmentSize(this.currentIndex())
    this.currentSegmentSize = fs.statSync(this.currentLog()).size
  }
}

RATS.prototype.append = function (obj, time, cb) {
  if (!cb) {
    cb = time // time is optional
    time = Math.round(Date.now() / 1000)
  }
  var data = new Buffer(JSON.stringify(Object.assign({}, {timestamp: time}, obj)) + '\n')
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

RATS.prototype.get = function (offset, cb) {
  var self = this

  fs.readdir(this.path, function (err, files) {
    if (err) return cb(err)

    var segmentOffsets = files.filter(isIndexFile).map(getSegmentOffset)
    var targetSegment
    for (var i = 0; i < segmentOffsets.length; i++) {
      if (segmentOffsets[i] <= offset) {
        if (i === segmentOffsets.length - 1 || segmentOffsets[i + 1] > offset) {
          targetSegment = segmentOffsets[i]
          break
        }
      }
    }
    if (targetSegment === undefined) return cb(new Error('offset not found'))

    // check if target segement contains the offset
    if ((targetSegment + segmentSize(self.indexFile(targetSegment)) - 1) < offset) return cb(new Error('offset not found'))

    getIndexItem(self.indexFile(targetSegment), offset, function (err, index) {
      if (err) return cb(err)

      getLogByIndex(self.logFile(targetSegment), index, cb)
    })
  })
}

RATS.prototype.currentIndex = function () {
  return this.indexFile(this.currentSegmentOffset)
}

RATS.prototype.currentLog = function () {
  return this.logFile(this.currentSegmentOffset)
}

RATS.prototype.indexFile = function (segmentOffset) {
  return path.join(this.path, `${segmentOffset}.index`)
}

RATS.prototype.logFile = function (segmentOffset) {
  return path.join(this.path, `${segmentOffset}.log`)
}

RATS.prototype.range = function (start, end, cb) {
  if (!cb) {
    cb = end
    end = undefined
  }
  var self = this
  fs.readdir(this.path, function (err, files) {
    if (err) return cb(err)

    files = files.filter(isIndexFile).map(fullPath)
    async.mapSeries(files, getHeader, function (err, segments) {
      if (err) return cb(err)

      segments = segments.sort(byBaseTimestamp)
      var segmentStart
      var segmentEnd = segments.length
      for (var i = 0; i < segments.length; i++) {
        var ts = uint64be.decode(segments[i].header.baseTimestamp)
        if (!segmentStart && ts >= start) {
          segmentStart = i
        }

        if (end && ts < end) {
          segmentEnd = i
        }
      }

      segments = segments.slice(segmentStart, segmentEnd + 1)

      cb(null, getLogItemInRange(segments, start, end))
    })
  })

  function fullPath (file) {
    return path.join(self.path, file)
  }

  function byBaseTimestamp (x, y) {
    return x.header.baseTimestamp - y.header.baseTimestamp
  }

  function getLogItemInRange (segments, start, end) {
    var streams = segments
        .reverse()
        .map(function (seg) { return fs.createReadStream(findLog(seg.file)) })
    if (streams.length > 1) {
      streams = hl.concat.apply(this, streams)
    } else {
      streams = hl(streams[0])
    }
    return streams.pipe(
      hl.pipeline(
        ndjson.parse(),
        hl.filter(function (data) { return data.timestamp >= start && (!end || data.timestamp < end) })
      ))
  }
}

function isIndexFile (file) {
  return file.endsWith('.index')
}

function getSegmentOffset (indexFile) {
  var offset = +path.basename(indexFile).match(/^(\d+)\.index/)[1]
  return offset
}

function getHeader (indexFile, cb) {
  var buf = new Buffer(HEADER_SIZE)
  fs.read(fs.openSync(indexFile, 'r'), buf, 0, HEADER_SIZE, 0, function (err, bytesRead, buf) {
    if (err) return cb(err)
    var header = messages.Header.decode(buf)

    cb(null, {file: indexFile, header: header})
  })
}

function findLog (indexFile) {
  return indexFile.replace(/\.index$/, '.log')
}

function segmentSize (indexFile) {
  return (fs.statSync(indexFile).size - HEADER_SIZE) / INDEX_ITEM_SIZE
}

function getIndexItem (indexFile, offset, cb) {
  var buf = new Buffer(INDEX_ITEM_SIZE)
  var pos = (offset - getSegmentOffset(indexFile)) * INDEX_ITEM_SIZE + HEADER_SIZE
  fs.read(fs.openSync(indexFile, 'r'), buf, 0, INDEX_ITEM_SIZE, pos, function (err, bytesRead, buf) {
    if (err) return cb(err)
    var index = messages.Index.decode(buf)

    cb(null, index)
  })
}

function getLogByIndex (logFile, index, cb) {
  var buf = new Buffer(index.size)
  fs.read(fs.openSync(logFile, 'r'), buf, 0, index.size, index.position, function (err, bytesRead, buf) {
    if (err) return cb(err)
    var log = JSON.parse(buf)

    cb(null, log)
  })
}
