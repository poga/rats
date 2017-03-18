# rats.js

JavaScript implementation for [RATS](https://github.com/random-access-timeseries/spec).

`npm i rats-js`

## Usage

```js
var RATS = require('rats')

var rats = new RATS('./rats')
rats.append({foo: 'bar'}, function (err) {})
rats.currentOffset // === 1
rats.get(0, function (err, data) {})
rats.range(0, 100, function (err, list) {})
```

## API

#### `var rats = new RATS(path, opts)`

Create a new RATS at `path`. Options `opts` include:

* maxSegmentSize: maximum size of a segment. default:5MB

#### `rats.currentOffset`

Returns the current offset.

#### `rats.append(object, [time], cb(err))`

Append an `object` to the RATS. you can specify the timestamp of this append with `time` (unix time in seconds).

#### `rats.currentIndex()`

The index file currently in use.

#### `rats.currentLog()`

The log file currently in use.

#### `rats.get(offset, cb(err, data))`

Get the data at `offset`.

#### `rats.range(startTime, [endTime], cb(err,list))`

Get the data(s) in the range.



## License

The MIT License
