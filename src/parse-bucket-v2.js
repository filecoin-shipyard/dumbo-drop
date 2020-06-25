const parseFile = require('./parse-file-v2')
const { readFile, writeFile } = require('fs').promises
const { writeFileSync } = require('fs')
const limiter = require('./limiter')
const prettyBytes = require('pretty-bytes')
const logUpdate = require('log-update')
const bent = require('bent')
const head = bent('HEAD', 200, 403, 500 /* these are intermittent and retries tend to fix */)
const remainingTime = require('remaining-time')

const sleep = ts => new Promise(resolve => setTimeout(resolve, ts))

const AWS = require('aws-sdk')
const awsConfig = require('aws-config')
const sep = '\n\n\n\n\n\n\n\n'

const tb = 1024 * 1024 * 1024 * 1024

let latest

const skipItem = async (db, url, checkHead = false) => {
  const item = await db.getItem(url, ['url'])
  if (item && item.url) return true
  if (!checkHead) return false
  const resp = await head(url)
  if (resp.statusCode === 403) return true
  return false
}

const skipItems = async (db, urls, checkHead = false, force = false) => {
  const found = force ? new Set() : new Set(Object.keys(await db.getItems(urls)))
  const missing = urls.filter(u => !found.has(u))
  const promises = []
  if (checkHead) {
    for (const url of missing) {
      promises.push(head(url).then(resp => {
        if (resp.statusCode === 403) found.add(url)
      }))
    }
  }
  await Promise.all(promises)
  return found
}

// returns list of files in bucket
const ls = async function * (opts) {
  opts = { ...opts }
  let data
  do {
    const s3 = new AWS.S3({ ...awsConfig(), correctCloseSkew: true })
    data = await s3.listObjectsV2(opts).promise()
    yield * data.Contents
    if (!data.Contents.length) {
      return
    }
    opts.StartAfter = data.Contents[data.Contents.length - 1].Key
  } while (data.Contents.length)
}

const getURL = fileInfo => {
  if (fileInfo.Bucket.includes('.')) {
    return `https://s3.amazonaws.com/${fileInfo.Bucket}/${AWS.util.uriEscapePath(fileInfo.Key)}`
  } else {
    return `https://${fileInfo.Bucket}.s3.amazonaws.com/${AWS.util.uriEscapePath(fileInfo.Key)}`
  }
}

const inflight = []
const upperLimit = 1024 * 1024 * 912

// main entrypoint for parsing a bucket
const run = async (Bucket, Prefix, StartAfter, concurrency = 500, checkHead = false, force = false) => {
  const opts = { Bucket, Prefix, StartAfter }
  const limit = limiter(concurrency)
  const display = { Bucket, skipped: 0, skippedBytes: 0, complete: 0, processed: 0 }
  const blockBucket = process.env.DUMBO_BLOCK_STORE

  // if we have a state file, read it and resume processing from that point.  If no state
  // file initialize it to start a fresh run
  const stateFile = `.state-${Bucket}`
  const loadState = async () => {
    let f
    try {
      f = await readFile(stateFile)
    } catch (e) {
      return null
    }
    return JSON.parse(f.toString())
  }
  let state = await loadState(stateFile)
  if (state && state.startAfter) {
    opts.StartAfter = state.startAfter
    display.skippedBytes = state.completed
  } else {
    state = { completed: 0 }
  }
  // setup a timer to periodically save our current processing state so we can resume
  // if something goes wrong
  const saveState = async () => {
    state.startAfter = inflight.length ? inflight[0] : latest || state.startAfter
    const _state = {
      completed: display.processed + display.skippedBytes,
      startAfter: state.startAfter
    }
    return writeFileSync(stateFile, Buffer.from(JSON.stringify(_state)))
  }
  setInterval(async () => {
    await saveState()
  }, 10000)

  if (StartAfter) StartAfter = StartAfter.slice(0, StartAfter.length - 2)

  // TODO: check for table and create if missing
  const tableName = `dumbo-v2-${Bucket}`

  const db = require('./queries')(tableName)

  // Function to parse a single file
  const sizes = []
  const parse = async info => {
    if (info.Size) {
      if (force || !(await skipItem(db, info.url, checkHead))) {
        inflight.push(info.Key)
        await parseFile(tableName, blockBucket, info.url, info.Bucket, info.Size)
        display.complete += 1
        inflight.splice(inflight.indexOf(info.Key), 1)
        display.processed += info.Size
      } else {
        display.skippedBytes += info.Size
        display.skipped += 1
      }
    }
  }
  // interval timer to print out processing progress
  const interval = setInterval(() => {
    const outs = { ...display }
    sizes.push(outs.processed)
    if (sizes.length > 500000) sizes.shift()
    while (sizes.length && sizes[0] === 0) sizes.shift()
    outs.inflight = inflight.length
    outs.skippedBytes = prettyBytes(outs.skippedBytes)
    outs.processed = prettyBytes(outs.processed)
    outs.pendingWrites = parseFile.debug.pending
    outs.writesFreed = parseFile.debug.free
    let persec
    if (sizes.length) {
      persec = (sizes[sizes.length - 1] - sizes[0]) / sizes.length
      outs.perf = prettyBytes(persec) + ' per second'
    }
    outs.oldest = inflight[0]
    logUpdate(JSON.stringify(outs, null, 2))
  }, 1000)

  // function to parse multiple files at once
  let bulk = []
  const bulkLength = () => bulk.reduce((x, y) => x + y.Size, 0)
  const runBulk = async _bulk => {
    const files = {}
    const keyMap = {}
    _bulk = _bulk.map(info => {
      files[info.url] = info.Size
      keyMap[info.url] = info.Key
      return info
    })
    const found = await skipItems(db, Object.keys(files), checkHead, force)
    for (const url of found) {
      display.skippedBytes += files[url]
      delete files[url]
      display.skipped += 1
    }

    const urls = Object.keys(files)
    if (urls.length) {
      urls.forEach(url => inflight.push(keyMap[url]))
      await parseFile.files(tableName, blockBucket, files, opts.Bucket)
      urls.forEach(url => inflight.splice(inflight.indexOf(keyMap[url]), 1))
      display.complete += urls.length
      display.processed += Object.values(files).reduce((x, y) => x + y, 0)
    }
  }
  // get list of files from bucket and parse them through the limiter
  for await (let fileInfo of ls(opts)) {
    if (!fileInfo.Size) continue
    fileInfo = { ...fileInfo, ...opts }

    fileInfo.url = getURL(fileInfo)
    latest = fileInfo.Key

    // Bulking is fixes to either 1GB (to avoid Lambda timeout)
    // or to 100 entries since that is the batchGetItem limit anyway.
    // I ran a small test w/ 500 in the bulk set and parallel getItem
    // requests but it didn't make any difference.

    if (fileInfo.Size > upperLimit) {
      await limit(parse(fileInfo))
      await sleep(500)
      continue
    } else if (((bulkLength() + fileInfo.Size) > upperLimit) || bulk.length > 99) {
      await limit(runBulk(bulk))
      await sleep(500)
      bulk = []
    }
    bulk.push(fileInfo)
  }
  await limit(runBulk(bulk))
  await limit.wait()
  clearInterval(interval)
  return display
}

module.exports = run
module.exports.getURL = getURL
// run('prd-tnm', 'StagedProducts/')
