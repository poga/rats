var RATS = require('..')
var tape = require('tape')
var fs = require('fs')
var path = require('path')
var rimraf = require('rimraf')
var mkdirp = require('mkdirp')

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

      var index = fs.statSync(path.join(dir, '0.index'))
      t.equal(index.size, 15)
      var log = fs.statSync(path.join(dir, '0.log'))
      t.equal(log.size, 34)
      var data = JSON.parse(fs.readFileSync(path.join(dir, '0.log')))
      t.same(data.foo, 'bar')
      t.ok(data.time)
      t.end()

      rimraf(dir, function () {})
    })
  }
})
