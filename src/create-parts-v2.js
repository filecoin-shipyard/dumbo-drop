const AWS = require('./aws')
const awsConfig = require('aws-config')
const logUpdate = require('log-update')
const prettyBytes = require('pretty-bytes')
const lambda = require('./lambda')()
const limiter = require('./limiter')
const s3 = new AWS.S3({ ...awsConfig(), correctCloseSkew: true })

const output = { completed: 0, completedBytes: 0, inflight: 0, updateQueue: 0, largest: 0 }

const maxSize = 1024 * 1024 * 912
const sep = '\n\n\n\n\n\n\n\n\n\n'

const seen = new Set()

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

const lambdaName = process.env.DUMBO_CREATE_PART_LAMBDA
const createPartsRequest = async (opts, retries = 5) => {
  let ret
  try {
    ret = await lambda(lambdaName, opts)
  } catch (e) {
    await sleep(Math.floor(10000 * Math.random()))
    if (e.retries && retries) return createPartsRequest(opts, retries - 1)
    throw e
  }
  return ret
}

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

const allocations = []

const ls = db => {
  const attrs = ['carUrl', 'size', 'url', 'split']
  const params = db.mkquery(attrs, { true: true })
  params.FilterExpression = 'not #split = :true and attribute_not_exists(#carUrl)'
  params.ProjectionExpression = attrs.map(s => '#' + s).join(', ')
  return db.slowScan(params)
}

let lastUrl

const getUrls = async function * (db) {
  for await (const { url, size } of ls(db)) {
    if (size > maxSize) throw new Error('Part slice too large')
    if (size === maxSize) {
      yield [size, [url]]
      continue
    }
    let allocated = false
    for (let i = 0; i < allocations.length; i++) {
      const [_size, _urls] = allocations[i]
      const csize = _size + size
      if (csize < maxSize) {
        const entryUrls = [..._urls, url]
        const entry = [csize, entryUrls]
        if ((csize > (maxSize - (1024 * 1024))) || entryUrls.length > 2000) {
          allocations.splice(i, 1)
          yield entry
        } else {
          allocations[i] = entry
        }
        allocated = true
        break
      }
    }
    if (!allocated) allocations.push([size, [url]])
  }
  yield * allocations
}

let updateMutex = null

const createPart = async (bucket, db, urls, size) => {
  output.inflight++
  const files = await db.getItems(urls, 'parts', 'size')
  for (const [f, item] of Object.entries(files)) {
    files[f] = [item.parts, item.size]
  }

  const blockBucket = 'dumbo-v2-block-bucket'
  const query = { Bucket: `dumbo-v2-cars-${bucket}`, files, blockBucket }
  const resp = await createPartsRequest(query)
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

const run = async argv => {
  let interval
  if (!argv.silent) interval = setInterval(print, 1000)
  const { bucket, concurrency } = argv
  try {
    await s3.createBucket({ Bucket: `dumbo-v2-cars-${bucket}`, ACL: 'public-read' }).promise()
  } catch (e) { /* noop */ }

  const tableName = `dumbo-v2-${bucket}`

  const db = require('./queries')(tableName)

  const limit = limiter(concurrency)

  for await (const [size, urls] of getUrls(db, bucket)) {
    if (urls.length > output.largest) output.largest = urls.length
    await limit(createPart(bucket, db, urls, size))
    await sleep(50) // protect against max per second request limits
  }
  await limit.wait()
  if (interval) clearInterval(interval)
}
module.exports = run
module.exports.getUrls = getUrls
module.exports.ls = ls
