var RATS = require('..')
var tape = require('tape')
var fs = require('fs')
var path = require('path')
var rimraf = require('rimraf')
var mkdirp = require('mkdirp')
var messages = require('protocol-buffers')(fs.readFileSync(path.join(__dirname, '..', 'messages.proto')))

tape('append', function (t) {
  var dir = path.join('.', 'temp')
  mkdirp(dir, function (err) {
    t.error(err)
    test()
  })

  function test () {
    var rats = new RATS(dir)
    rats.append({foo: 'bar'}, function (err) {
      t.error(err)

      var indexFile = path.join(dir, '0.index')
      var logFile = path.join(dir, '0.log')
      var index = fs.statSync(indexFile)
      t.equal(index.size, 15)
      var log = fs.statSync(logFile)
      t.equal(log.size, 35)
      t.same(messages.Index.decode(fs.readFileSync(indexFile)), { offset: 0, position: 0, size: 35 })
      var data = JSON.parse(fs.readFileSync(logFile))
      t.same(data.foo, 'bar')
      t.ok(data.time)
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
        var index = fs.statSync(indexFile)
        t.equal(index.size, 15)
        var log = fs.statSync(logFile)
        t.equal(log.size, 35)
        t.same(messages.Index.decode(fs.readFileSync(indexFile)), { offset: 1, position: 0, size: 35 })
        var data = JSON.parse(fs.readFileSync(logFile))
        t.same(data.foo, 'baz')
        t.ok(data.time)

        rimraf(dir, function () {
          t.end()
        })
      })
    })
  }
})
