var tape = require('tape')
var fs = require('fs')
var path = require('path')
var messages = require('protocol-buffers')(fs.readFileSync(path.join(__dirname, '..', 'messages.proto')))
var uint64be = require('uint64be')
var RATS = require('..')

tape('constants', function (t) {
  t.same(messages.Header.encode({version: 1, baseTimestamp: uint64be.encode(2)}).length, RATS.HEADER_SIZE, 'HEADER_SIZE')
  t.same(messages.Index.encode({offset: 1, position: 2, size: 3}).length, RATS.INDEX_ITEM_SIZE, 'INDEX_ITEM_SIZE')
  t.end()
})
