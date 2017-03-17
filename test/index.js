var RATS = require('..')
var tape = require('tape')
var fs = require('fs')
var path = require('path')
var rimraf = require('rimraf')
var mkdirp = require('mkdirp')
var messages = require('protocol-buffers')(fs.readFileSync(path.join(__dirname, '..', 'messages.proto')))
var uint64be = require('uint64be')

tape('append', function (t) {
  var dir = path.join('.', 'temp')
  mkdirp(dir, function (err) {
    t.error(err)
    test()
  })

  function test () {
    var rats = new RATS(dir)
    var now = Math.round(Date.now() / 1000)
    rats.append({foo: 'bar'}, now, function (err) {
      t.error(err)

      var indexFile = path.join(dir, '0.index')
      var logFile = path.join(dir, '0.log')

      // check index and log size
      var index = fs.statSync(indexFile)
      t.equal(index.size, 29, 'index file size')
      var log = fs.statSync(logFile)
      t.equal(log.size, 14, 'log file size')

      // check decode index header
      var headerBuf = new Buffer(14)
      fs.readSync(fs.openSync(indexFile, 'r'), headerBuf, 0, 14)
      var header = messages.Header.decode(headerBuf)
      t.equal(header.version, 1, 'decode header version')
      t.same(header.baseTimestamp, uint64be.encode(now), 'decode header base timestamp')

      // check decode index item
      var indexBuf = new Buffer(15)
      fs.readSync(fs.openSync(indexFile, 'r'), indexBuf, 0, 15, 14)
      t.same(messages.Index.decode(indexBuf), { offset: 0, position: 0, size: 14 }, 'decode index item')

      // check decode log
      var data = JSON.parse(fs.readFileSync(logFile))
      t.same(data, {foo: 'bar'}, 'decode log')
      rimraf(dir, function () {
        t.end()
      })
    })
  }
})

tape('segment', function (t) {
  var dir = path.join('.', 'temp')
  mkdirp(dir, function (err) {
    t.error(err)
    test()
  })

  function test () {
    var rats = new RATS(dir, {maxSegmentSize: 0})
    rats.append({foo: 'bar'}, function (err) {
      t.error(err)

      rats.append({foo: 'baz'}, function (err) {
        t.error(err)

        var indexFile = path.join(dir, '1.index')
        var logFile = path.join(dir, '1.log')

        // check index and log size
        var index = fs.statSync(indexFile)
        t.equal(index.size, 29, 'index size')
        var log = fs.statSync(logFile)
        t.equal(log.size, 14, 'log size')

        // check decode index header
        var headerBuf = new Buffer(14)
        fs.readSync(fs.openSync(indexFile, 'r'), headerBuf, 0, 14)
        var header = messages.Header.decode(headerBuf)
        t.equal(header.version, 1, 'decode header version')
        t.ok(header.baseTimestamp, 'decode header base timestamp')

        // check decode index item
        var indexBuf = new Buffer(15)
        fs.readSync(fs.openSync(indexFile, 'r'), indexBuf, 0, 15, 14)
        t.same(messages.Index.decode(indexBuf), { offset: 1, position: 0, size: 14 }, 'decode index item')

        // check decode log
        var data = JSON.parse(fs.readFileSync(logFile))
        t.same(data, {foo: 'baz'}, 'decode log')

        rimraf(dir, function () {
          t.end()
        })
      })
    })
  }
})

tape('existing data', function (t) {
  var dir = path.join('.', 'test', 'fixtures', 'data')
  mkdirp(dir, function (err) {
    t.error(err)
    test()
  })

  function test () {
    var rats = new RATS(dir, {maxSegmentSize: 20})
    t.same(rats.currentOffset, 3)
    t.same(rats.currentSegmentSize, 14)
    t.same(rats.currentSegmentOffset, 2)
    t.end()
  }
})
