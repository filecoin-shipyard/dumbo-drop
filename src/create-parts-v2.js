const AWS = require('./aws')
const awsConfig = require('aws-config')
const logUpdate = require('log-update')
const prettyBytes = require('pretty-bytes')
const lambda = require('./lambda')()
const limiter = require('./limiter')
const s3 = new AWS.S3({ ...awsConfig(), correctCloseSkew: true })

const output = { completed: 0, completedBytes: 0, inflight: 0, updateQueue: 0, largest: 0 }

// TODO: rename to MAX_CAR_FILE_SIZE
const maxSize = 1024 * 1024 * 912

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

const lambdaName = process.env.DUMBO_CREATE_PART_LAMBDA

// print out current status (called by interval function set below)
const history = []
const print = () => {
  history.push(output.completedBytes)
  while (history.length > 900) {
    history.shift()
  }
  const outs = { ...output }
  outs.completedBytes = prettyBytes(outs.completedBytes)
  outs.rate = prettyBytes((history[history.length - 1] - history[0]) / history.length) + ' per second'
  logUpdate(JSON.stringify(outs, null, 2))
}

// array of objects which contain a total size and list of file part/slices
// that add up to that size.  This is used to collect file parts/slices together
// to generate car files near the max car file size
const allocations = []

// get list of files to turn into car parts
const ls = db => {
  const attrs = ['carUrl', 'size', 'url', 'split']
  const params = db.mkquery(attrs, { true: true })
  params.FilterExpression = 'not #split = :true and attribute_not_exists(#carUrl)'
  params.ProjectionExpression = attrs.map(s => '#' + s).join(', ')
  return db.slowScan(params)
}

// iterator function that returns an array of files or file slices
// to be aggregated together into a single car file.  
const getItemsForCARFile = async function * (db) {
  for await (const { url, size } of ls(db)) {
    if (size > maxSize) throw new Error('Part slice too large')
    // for files or file slices of carfile maxsize, encode it into a single car file
    if (size === maxSize) {
      yield [size, [url]]
      continue
    }
    let allocated = false
    // iterate through all existing allocation entries and add this file or file part
    // to an existing entry as long as it doesn't exceed the max car file size.  Also
    // return all files/file parts if we meet the criteria to generate a complete car file
    for (let i = 0; i < allocations.length; i++) {
      const [_size, _urls] = allocations[i]
      const csize = _size + size
      // if adding this file part/slice does not exceed max car file size..
      if (csize < maxSize) {
        // merge this file part/slice into the allocation entry
        const entryUrls = [..._urls, url]
        const entry = [csize, entryUrls]

        // check to see if we should create a car file now
        // TODO: replace 1024*1024 with constant (e.g. MAX_BLOCK_SIZE)
        // TODO: Document why 2000 is the upper limit on number of urls
        if ((csize > (maxSize - (1024 * 1024))) || entryUrls.length > 2000) {
          // yes - remove the accumulated entries and return them
          allocations.splice(i, 1)
          yield entry
        } else {
          // no - replace the allocated entry with this new aggregated one.
          allocations[i] = entry
        }
        // file part/slice has been allocated, break out of loop
        allocated = true
        break
      }
    }
    if (!allocated) allocations.push([size, [url]])
  }
  // iteration complete, return the list of file parts/slices 
  // to be encoded into the last car file
  yield * allocations
}

let updateMutex = null

// creates a car file from a list of files or file slices
const createPart = async (bucket, db, urls, size) => {
  output.inflight++
  const files = await db.getItems(urls, 'parts', 'size')
  for (const [f, item] of Object.entries(files)) {
    files[f] = [item.parts, item.size]
  }

  const blockBucket = process.env.DUMBO_BLOCK_BUCKET
  const query = { Bucket: `dumbo-v2-cars-${bucket}`, files, blockBucket }
  const resp = await lambda(lambdaName, query)
  const { results, details, root } = resp
  const carUrl = details.Location
  const updates = []
  for (const [key, _root] of Object.entries(results)) {
    updates.push({ key, root: [root, ..._root], carUrl })
  }
  output.updateQueue++
  while (updateMutex) {
    await updateMutex
  }
  output.updateQueue--
  updateMutex = db.bulkUpdate(updates)
  await updateMutex
  updateMutex = null
  output.completed++
  output.completedBytes += size

  output.inflight--
}

// entry point for file
const run = async argv => {
  let interval
  // setup interval to print out progress/status
  if (!argv.silent) interval = setInterval(print, 1000)

  // create bucket for cars
  const { bucket, concurrency } = argv
  try {
    await s3.createBucket({ Bucket: `dumbo-v2-cars-${bucket}`, ACL: 'public-read' }).promise()
  } catch (e) { /* noop */ }

  const tableName = `dumbo-v2-${bucket}`

  const db = require('./queries')(tableName)

  const limit = limiter(concurrency)

  // get list of urls to files or file slices to process and create cars from them
  // using the limiter
  for await (const [size, urls] of getItemsForCARFile(db, bucket)) {
    console.log('size=', size)
    console.log('urls=',urls)
    if (urls.length > output.largest) output.largest = urls.length
    await limit(createPart(bucket, db, urls, size))
    await sleep(50) // protect against max per second request limits
  }
  await limit.wait()
  if (interval) clearInterval(interval)
}
module.exports = run
module.exports.getItemsForCARFile = getItemsForCARFile
module.exports.ls = ls
