# rats.js

JavaScript implementation for [RATS](https://github.com/random-access-timeseries/spec).

`npm i rats-js`

## Usage

```js
var RATS = require('rats')

var rats = new RATS('./rats')
rats.append({foo: 'bar'}, function (err) {

})
```

## License

The MIT License
