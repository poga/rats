var RATS = require('..')
var tape = require('tape')
var path = require('path')
var rimraf = require('rimraf')
var mkdirp = require('mkdirp')
var collect = require('collect-stream')

tape('range', function (t) {
  var dir = path.join('.', 'temp')
  mkdirp(dir, function (err) {
    t.error(err)
    prepare()
  })
  var rats
  var t1 = Math.round(Date.now() / 1000)

  function prepare () {
    rats = new RATS(dir, t1, {maxSegmentSize: 20})
    rats.append({foo: 'bar'}, function (err) {
      t.error(err)
      t.equal(rats.currentOffset, 1, 'current offset')

      rats.append({foo: 'baz'}, t1 + 1, function (err) {
        t.error(err)
        t.equal(rats.currentOffset, 2, 'current offset')

        rats.append({foo: 'bzz'}, t1 + 2, function (err) {
          t.error(err)
          t.equal(rats.currentOffset, 3, 'current offset')

          test()
        })
      })
    })
  }

  function test () {
    rats.range(t1, t1 + 1, function (err, stream) {
      t.error(err)
      collect(stream, function (err, data) {
        t.error(err)
        t.same(data, [{foo: 'bar', timestamp: t1}])
        test2()
      })
    })
  }

  function test2 () {
    rats.range(t1, t1 + 2, function (err, stream) {
      t.error(err)
      collect(stream, function (err, data) {
        t.error(err)
        t.same(data, [{foo: 'bar', timestamp: t1}, {foo: 'baz', timestamp: t1 + 1}])
        test3()
      })
    })
  }

  function test3 () {
    rats.range(t1, t1 + 3, function (err, stream) {
      t.error(err)
      collect(stream, function (err, data) {
        t.error(err)
        t.same(data, [
           {foo: 'bar', timestamp: t1},
           {foo: 'baz', timestamp: t1 + 1},
           {foo: 'bzz', timestamp: t1 + 2}
        ])
        test4()
      })
    })
  }

  function test4 () {
    rats.range(t1, t1 + 4, function (err, stream) {
      t.error(err)
      collect(stream, function (err, data) {
        t.error(err)
        t.same(data, [
           {foo: 'bar', timestamp: t1},
           {foo: 'baz', timestamp: t1 + 1},
           {foo: 'bzz', timestamp: t1 + 2}
        ])
        test5()
      })
    })
  }

  function test5 () {
    rats.range(t1 + 5, t1 + 6, function (err, stream) {
      t.error(err)
      collect(stream, function (err, data) {
        t.error(err)
        t.same(data, [])
        test6()
      })
    })
  }

  function test6 () {
    rats.range(t1, t1, function (err, stream) {
      t.error(err)
      collect(stream, function (err, data) {
        t.error(err)
        t.same(data, [])
        rimraf(dir, function () {
          t.end()
        })
      })
    })
  }
})

tape('range without end time', function (t) {
  var dir = path.join('.', 'temp')
  mkdirp(dir, function (err) {
    t.error(err)
    prepare()
  })
  var rats
  var t1 = Math.round(Date.now() / 1000)

  function prepare () {
    rats = new RATS(dir, t1, {maxSegmentSize: 20})
    rats.append({foo: 'bar'}, function (err) {
      t.error(err)
      t.equal(rats.currentOffset, 1, 'current offset')

      rats.append({foo: 'baz'}, t1 + 1, function (err) {
        t.error(err)
        t.equal(rats.currentOffset, 2, 'current offset')

        rats.append({foo: 'bzz'}, t1 + 2, function (err) {
          t.error(err)
          t.equal(rats.currentOffset, 3, 'current offset')

          test()
        })
      })
    })
  }

  function test () {
    rats.range(t1, function (err, stream) {
      t.error(err)
      collect(stream, function (err, data) {
        t.error(err)
        t.same(data, [
          {foo: 'bar', timestamp: t1},
          {foo: 'baz', timestamp: t1 + 1},
          {foo: 'bzz', timestamp: t1 + 2}
        ])
        rimraf(dir, function () {
          t.end()
        })
      })
    })
  }
})
