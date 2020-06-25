const lambda = require('./lambda')()

const functionName = process.env.DUMBO_PARSE_FILE_LAMBDA

const limit = 1024 * 1024 * 912

let writeMutex

const debug = { pending: 0, free: 0 }

const sep = '\n\n\n\n\n\n\n\n\n\n'

const saveFile = async (db, url, dataset, parts, size) => {
  const item = { url, size, dataset, parts }
  debug.pending++
  while (writeMutex) {
    await writeMutex
  }
  writeMutex = db.putItem(item)
  const resp = await writeMutex
  writeMutex = null
  debug.free++
  debug.pending--
  return resp
}

const saveSplits = async (db, url, dataset, splits, size) => {
  /*
  DynamoDB has a 400K limit on the size of an entry. With
  the size of hash strings that means we can't store the
  parts of a file over 7GB.

  Since we're already breaking up the chunking of files
  over 1GB it makes sense to store the parts split by
  the same limit. This is all a bit of a hack, but it
  was going to be necessary to break up large files
  at some boundary point anyway in order to spread
  files over 32GB into multiple .car files later on.
  */
  let i = 0
  const originalSize = size
  const _bulkSize = splits.length + 1
  debug.pending += _bulkSize
  while (writeMutex) {
    await writeMutex
  }
  writeMutex = new Promise(resolve => {
    const writes = []
    for (const parts of splits) {
      const _size = size
      size -= limit
      const l = _size < limit ? _size : limit
      const item = { size: l, dataset, parts, url: `::split::${url}::${i}` }
      writes.push(db.putItem(item))
      i++
    }
    resolve(Promise.all(writes))
  })
  const writeResponses = await writeMutex
  const item = { url, size: originalSize, dataset, split: true }
  writeMutex = db.putItem(item)
  const resp = await writeMutex
  writeMutex = null
  debug.free += _bulkSize
  debug.pending -= _bulkSize
  return [resp, ...writeResponses]
}

const parseFile = async (tableName, blockBucket, url, dataset, size) => {
  const db = require('./queries')(tableName)
  let opts = { url, blockBucket }
  let parts = []
  if (size < limit) {
    parts = await lambda(functionName, opts)
    const resp = await saveFile(db, url, dataset, parts, size)
    return resp
  } else {
    let i = 0
    const splits = []
    while (i < size) {
      opts = { url, headers: { Range: `bytes=${i}-${(i + limit) - 1}` }, blockBucket }
      const chunks = await lambda(functionName, opts)
      splits.push(chunks)
      i += limit
    }
    const resp = await saveSplits(db, url, dataset, splits, size)
    return resp
  }
}

const parseFiles = async (tableName, blockBucket, files, dataset) => {
  const db = require('./queries')(tableName)
  const urls = Object.keys(files)
  const opts = { urls, blockBucket }
  const resp = await lambda(functionName, opts)
  const writes = []
  for (const [url, parts] of Object.entries(resp)) {
    writes.push(saveFile(db, url, dataset, parts, files[url]))
  }
  return Promise.all(writes)
}

module.exports = parseFile
module.exports.files = parseFiles
module.exports.debug = debug

// parseFile('https://prd-tnm.s3.amazonaws.com/StagedProducts/Contours/National/GDB/Elevation_National_GDB.zip', 'prd-tnm', 92892240919)
