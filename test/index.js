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
      t.equal(rats.currentOffset, 1, 'current offset')

      var indexFile = path.join(dir, '0.index')
      var logFile = path.join(dir, '0.log')

      // check index and log size
      var index = fs.statSync(indexFile)
      t.equal(index.size, 29, 'index file size')
      var log = fs.statSync(logFile)
      t.equal(log.size, 37, 'log file size')

      // check decode index header
      var headerBuf = new Buffer(14)
      fs.readSync(fs.openSync(indexFile, 'r'), headerBuf, 0, 14)
      var header = messages.Header.decode(headerBuf)
      t.equal(header.version, 1, 'decode header version')
      t.same(header.baseTimestamp, uint64be.encode(now), 'decode header base timestamp')

      // check decode index item
      var indexBuf = new Buffer(15)
      fs.readSync(fs.openSync(indexFile, 'r'), indexBuf, 0, 15, 14)
      t.same(messages.Index.decode(indexBuf), { offset: 0, position: 0, size: 37 }, 'decode index item')

      // check decode log
      var data = JSON.parse(fs.readFileSync(logFile))
      // timestamp should be injected
      t.same(data, {foo: 'bar', timestamp: now}, 'decode log')
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
      t.equal(rats.currentOffset, 1, 'current offset')

      rats.append({foo: 'baz'}, function (err) {
        t.error(err)
        t.equal(rats.currentOffset, 2, 'current offset')

        var indexFile = path.join(dir, '1.index')
        var logFile = path.join(dir, '1.log')

        // check index and log size
        var index = fs.statSync(indexFile)
        t.equal(index.size, 29, 'index size')
        var log = fs.statSync(logFile)
        t.equal(log.size, 37, 'log size')

        // check decode index header
        var headerBuf = new Buffer(14)
        fs.readSync(fs.openSync(indexFile, 'r'), headerBuf, 0, 14)
        var header = messages.Header.decode(headerBuf)
        t.equal(header.version, 1, 'decode header version')
        t.ok(header.baseTimestamp, 'decode header base timestamp')

        // check decode index item
        var indexBuf = new Buffer(15)
        fs.readSync(fs.openSync(indexFile, 'r'), indexBuf, 0, 15, 14)
        t.same(messages.Index.decode(indexBuf), { offset: 1, position: 0, size: 37 }, 'decode index item')

        // check decode log
        var data = JSON.parse(fs.readFileSync(logFile))
        t.same(data.foo, 'baz', 'decode log')
        t.ok(data.timestamp, 'injected timestamp')

        rimraf(dir, function () {
          t.end()
        })
      })
    })
  }
})

tape('get', function (t) {
  var dir = path.join('.', 'temp')
  mkdirp(dir, function (err) {
    t.error(err)
    test()
  })

  function test () {
    var rats = new RATS(dir)
    var t1 = Math.round(Date.now() / 1000)
    rats.append({foo: 'bar'}, t1, function (err) {
      t.error(err)
      rats.append({foo: 'baz'}, t1 + 1, function (err) {
        t.error(err)

        rats.get(0, function (err, data) {
          t.error(err)
          t.same(data, { foo: 'bar', timestamp: t1 })
          rats.get(1, function (err, data) {
            t.error(err)
            t.same(data, { foo: 'baz', timestamp: t1 + 1 })
            done()
          })
        })
      })
    })
  }

  function done () {
    rimraf(dir, function () {
      t.end()
    })
  }
})

tape('get invalid offset', function (t) {
  var dir = path.join('.', 'temp')
  mkdirp(dir, function (err) {
    t.error(err)
    test()
  })

  function test () {
    var rats = new RATS(dir)
    var t1 = Math.round(Date.now() / 1000)
    rats.append({foo: 'bar'}, t1, function (err) {
      t.error(err)
      rats.get(1, function (err, data) {
        t.ok(err)
        rats.get(-1, function (err, data) {
          t.ok(err)
          done()
        })
      })
    })
  }

  function done () {
    rimraf(dir, function () {
      t.end()
    })
  }
})

tape('inject timestamp', function (t) {
  var dir = path.join('.', 'temp')
  mkdirp(dir, function (err) {
    t.error(err)
    test()
  })

  function test () {
    var rats = new RATS(dir)
    rats.append({foo: 'bar', timestamp: 1234}, function (err) {
      t.error(err)
      t.equal(rats.currentOffset, 1, 'current offset')

      rats.append({foo: 'baz'}, function (err) {
        t.error(err)
        t.equal(rats.currentOffset, 2, 'current offset')

        rats.get(0, function (err, data) {
          t.error(err)
          t.same(data, { foo: 'bar', timestamp: 1234 })
          rimraf(dir, function () {
            t.end()
          })
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
    var rats = new RATS(dir)
    t.same(rats.currentOffset, 3)
    t.same(rats.currentSegmentSize, 14)
    t.same(rats.currentSegmentOffset, 2)
    t.end()
  }
})
