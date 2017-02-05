var {counterEncoder, fn, count, readHeader, read, gaugeEncoder} = require('..')
var tape = require('tape')
var streamify = require('stream-array')
var pump = require('pump')
var fs = require('fs')
var serialize = require('../serialize')

tape('encode counter', function (t) {
  pump(streamify([[0, 1], [1, 2], [2, 3]]), counterEncoder(), fs.createWriteStream(fn('test_encode')), function (err) {
    t.error(err)

    t.same(fs.statSync('test_encode.rats').size, 56) // 42 bytes header + 14 bytes per record

    var buf = fs.readFileSync('test_encode.rats')

    var header = serialize.decodeHeader(buf.slice(0, 42))
    t.same(header, {
      version: 0,
      metricType: 0,
      baseTs: 0,
      baseValue: 1,
      baseTsDelta: 1,
      baseValueDelta: 1
    })
    t.same(serialize.decodeRecord(header, 2, buf.slice(42, 56)), [2, 3])

    fs.unlinkSync('test_encode.rats')
    t.end()
  })
})

tape('count counter', function (t) {
  pump(streamify([[0, 1], [1, 2], [2, 3]]), counterEncoder(), fs.createWriteStream(fn('test_encode')), function (err) {
    t.error(err)

    t.same(count('test_encode.rats'), 3) // 42 bytes header + 14 bytes per record
    fs.unlinkSync('test_encode.rats')
    t.end()
  })
})

tape('readHeader counter', function (t) {
  pump(streamify([[0, 1], [1, 2], [2, 3]]), counterEncoder(), fs.createWriteStream(fn('test_encode')), function (err) {
    t.error(err)

    fs.open('test_encode.rats', 'r', function (err, fd) {
      t.error(err)
      readHeader(fd, function (err, header) {
        t.error(err)
        t.same(header, {
          version: 0,
          metricType: 0,
          baseTs: 0,
          baseValue: 1,
          baseTsDelta: 1,
          baseValueDelta: 1
        })
        fs.unlinkSync('test_encode.rats')
        t.end()
      })
    })
  })
})

tape('read counter', function (t) {
  pump(streamify([[0, 1], [1, 2], [2, 3]]), counterEncoder(), fs.createWriteStream(fn('test_encode')), function (err) {
    t.error(err)

    fs.open('test_encode.rats', 'r', function (err, fd) {
      t.error(err)
      readHeader(fd, function (err, header) {
        t.error(err)
        read(fd, header, 0, function (err, record) {
          t.error(err)
          t.same(record, [0, 1])
          read(fd, header, 1, function (err, record) {
            t.error(err)
            t.same(record, [1, 2])
            read(fd, header, 2, function (err, record) {
              t.error(err)
              t.same(record, [2, 3])
              fs.unlinkSync('test_encode.rats')
              t.end()
            })
          })
        })
      })
    })
  })
})

tape('encode gauge', function (t) {
  pump(streamify([[0, 1], [1, 2], [2, 3]]), gaugeEncoder(), fs.createWriteStream(fn('test_encode')), function (err) {
    t.error(err)

    t.same(fs.statSync('test_encode.rats').size, 56) // 42 bytes header + 14 bytes per record

    var buf = fs.readFileSync('test_encode.rats')

    var header = serialize.decodeHeader(buf.slice(0, 42))
    t.same(header, {
      version: 0,
      metricType: 1,
      baseTs: 0,
      baseValue: 1,
      baseTsDelta: 1,
      baseValueDelta: 1
    })
    t.same(serialize.decodeRecord(header, 2, buf.slice(42, 56)), [2, 3])

    fs.unlinkSync('test_encode.rats')
    t.end()
  })
})

tape('count gauge', function (t) {
  pump(streamify([[0, 1], [1, 2], [2, 3]]), gaugeEncoder(), fs.createWriteStream(fn('test_encode')), function (err) {
    t.error(err)

    t.same(count('test_encode.rats'), 3) // 42 bytes header + 14 bytes per record
    fs.unlinkSync('test_encode.rats')
    t.end()
  })
})

tape('readHeader gauge', function (t) {
  pump(streamify([[0, 1], [1, 2], [2, 3]]), gaugeEncoder(), fs.createWriteStream(fn('test_encode')), function (err) {
    t.error(err)

    fs.open('test_encode.rats', 'r', function (err, fd) {
      t.error(err)
      readHeader(fd, function (err, header) {
        t.error(err)
        t.same(header, {
          version: 0,
          metricType: 1,
          baseTs: 0,
          baseValue: 1,
          baseTsDelta: 1,
          baseValueDelta: 1
        })
        fs.unlinkSync('test_encode.rats')
        t.end()
      })
    })
  })
})

tape('read gauge', function (t) {
  pump(streamify([[0, 1], [1, 2], [2, 3]]), gaugeEncoder(), fs.createWriteStream(fn('test_encode')), function (err) {
    t.error(err)

    fs.open('test_encode.rats', 'r', function (err, fd) {
      t.error(err)
      readHeader(fd, function (err, header) {
        t.error(err)
        read(fd, header, 0, function (err, record) {
          t.error(err)
          t.same(record, [0, 1])
          read(fd, header, 1, function (err, record) {
            t.error(err)
            t.same(record, [1, 2])
            read(fd, header, 2, function (err, record) {
              t.error(err)
              t.same(record, [2, 3])
              fs.unlinkSync('test_encode.rats')
              t.end()
            })
          })
        })
      })
    })
  })
})
