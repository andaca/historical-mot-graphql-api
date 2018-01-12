const fs = require('fs')
const readline = require('readline')

const Box = x => ({
    fold: fn => fn(x),
    inspect: () => `Box(${x})`,
    map: fn => Box(fn(x)),
})

const Left = x => ({
    chain: fn => Left(x),
    fold: (onError, onSuccess) => onError(x),
    inspect: () => `Left(${x})`,
    map: fn => Left(x),
})

const Right = x => ({
    chain: fn => fn(x),
    fold: (onError, onSuccess) => onSuccess(x),
    inspect: () => `Right(${x})`,
    map: fn => Right(fn(x)),
})

const fromNullable = x =>
    x != null ? Right(x) : Left(null)

const tryCatch = fn => {
    try {
        return Right(fn())
    } catch (e) {
        return Left(e)
    }
}


const objZip = (keys, vals) => {
    vals = vals.slice()
    return keys.reduceRight((obj, key) => {
        obj[key] = vals.pop()
        return obj
    },{})
}

const handleData = fname => {
    batchDoer = BatchDoer(5, console.log)
    tryCatch(_ => fs.createReadStream(fname))
    .fold(
        error => (console.log(error), process.exit(1)),
        stream => readline.createInterface(stream)
    )
    .on('line', line => {
        line = line.split('|')
        if (typeof(cols) == 'undefined') 
            cols = line 
        else batchDoer.push(objZip(cols, line))
    })
}

const BatchDoer = (n, fn) => ({
    arr: [],
    push: x => {
        arr.push(x)
        if (arr.size == n) {
            doit()
            arr = []
        }
    },
    doit: () => fn(arr),
    finish: () => arr && doit()
})

const fname = process.argv[2] ||
    (console.log('filename required'), process.exit(1))
handleData(fname)