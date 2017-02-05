# rats.js

JavaScript implementation for [RATS](https://github.com/random-access-timeseries/spec).

`npm i rats-js`

## Usage

```js
var rats = require('rats')
var pump = require('pump')
var streamify = require('streamify')

// encode a timeseries to file
var now = Date.now()
var source = streamify([[now, 1], [now + 1000, 2], [now + 2000, 3]])

// open a file for given metric name and labels
var filename = rats.fn('example', {type: 'GET'})
var out = fs.createWriteStream(filename)

// create a encoder stream
var encoder = rats.counterEncoder()
// encode!
pump(source, encoder, out, function (err) {
  // done!
})

// decode

// how many records in the file?
rats.count(filename) // === 3

// decode header
rats.readHeader(filename, function (err, header) {

})

// read record by index
rats.read(filename, 0, function (err, record) {
  // records === [now, 1]
})
```

## License

The MIT License
