const fs = require('fs')
const readline = require('readline')


const attempt = fn => {
  try { return fn() }
  catch(_) { return null }
}


const objZip = (keys, vals) => {
  // Takes two arrays, the first array values are used for the object keys, the second for corresponding values.
  vals = vals.slice()
  return keys.reduceRight((obj, key) => {
    obj[key] = vals.pop()
    return obj
  }, {})
}


const objectify = row => 
  // When the first row is recieved, null is returned. All subsequent rows are returned as an object with the first row values as keys.
  typeof(this.cols) == 'undefined' ? (this.cols = row, null)
    : objZip(this.cols, row)


const lineStream = fname => {
  // Attempts to return a readline stream for the filename. If an error occurs, null is returned.
  const s = attempt(_=> fs.createReadStream(fname))
  return s && readline.createInterface(s)
}


(_ => {
  const stream = lineStream(process.argv[2]) ||
    (console.log('line read stream could not be created'), process.exit(1))

  stream.on('line', line => {
    [line]
      .map(x => x.split('|'))
      .map(x => objectify(x))
      .map(x => x && console.log(x))
  })
})()

