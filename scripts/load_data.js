const fs = require('fs')
const aws = require('aws-sdk')
const stream = require('stream')

const DDB_CFG = {
  tableName: 'testTable'
}

const attempt = fn => {
  try { return fn() }
  catch(_) { return null }
}


const zip = (keys, vals) =>  
  keys.reduceRight((obj, key) => (
    { [key]: vals.pop(), ...obj }
  ), {})


const objectify = row => 
  !this.cols ? (this.cols = row, null) : zip(this.cols, row)


const lineStream = fname => {
  const s = attempt(_=> fs.createReadStream(fname))
  return s && s.pipe(StreamSplitter('\n'))
}



const makePutItem = data => ({
  test_id:        { 'S': data.test_id },
  test_date:      { 'N': new Date(data.test_date).valueOf() },
  test_class_id:  { 'S': data.test_class_id},
  test_type:      { 'S': data.test_type },
  test_result:    { 'S': data.test_result },
  test_mileage:   { 'N': new Number(data.test_mileage) },
  postcode_area:  { 'S': data.postcode_area },
  make:           { 'S': data.make },
  model:          { 'S': data.model },
  colour:         { 'S': data.colour },
  fuel_type:      { 'S': data.fuel_type },
  cylinder_capacity: { 'N': new Number(data.cylinder_capacity) },
  first_use_date: { 'N': new Date(data.first_use_date).valueOf() },
})


const reqMaker = new stream.Transform({
  objectMode: true,
  transform(chunk, encoding, callback) {
    [chunk.toString()]
      .map(x => x.split('|'))
      .map(x => objectify(x))
      .filter(x => !!x)
      .map(x => makePutItem(x))
      .map(x => {x && this.push(x)})
    callback()
  }
})


const reqBatcher = new stream.Transform({
  objectMode: true,

  transform(x, _, callback) {
    this.items  || (this.items= [])
    this.nItems || (this.nItems=0)

    this.items.push({RequestItem: x})
    if (++this.nItems === 500) {
      this.push({[DDB_CFG.tableName]: this.items})
      this.items = null
      this.nItems = 0
    }
    callback()
  },

  flush(callback) {
    this.items && callback(null, 
      {[DDB_CFG.tableName]: this.items})
  }
})


const makeReqSender = ddb => 
  new stream.Readable({
    objectMode: true,

    read(x, _, callback) {
      ddb.batchWriteItem(x, callback)
    }
  })


const consoleLogger = new stream.Writable({
  objectMode: true,
  write(x, _, callback) {
    console.log(x)
    callback()
  }
})


function main() {
  const lineS = lineStream(process.argv[2]) ||
    (console.log('line read stream could not be created'), process.exit(1))
  
  const reqSender = makeReqSender(new aws.DynamoDB())
  lineS
    .pipe(reqMaker)
    .pipe(reqBatcher)
    .pipe(consoleLogger)
    .pipe(reqSender)
}
main()
