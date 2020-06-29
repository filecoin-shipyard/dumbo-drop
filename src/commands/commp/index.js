const AWS = require('aws-sdk')
const awsConfig = require('aws-config')
const { putItem, getItem } = require('../../queries')(process.env.DUMBO_COMMP_TABLE)
const limiter = require('../../limiter')
const lambda = require('../../lambda').raw
const logUpdate = require('log-update')
const prettyBytes = require('pretty-bytes')

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

const output = { fails: 0, skips: 0, skippedBytes: 0, completed: 0, completedBytes: 0, inflight: 0 }
const history = []

const print = () => {
  history.push(output.completedBytes)
  while (history.length > 600) {
    history.shift()
  }
  const outs = { ...output }
  outs.completedBytes = prettyBytes(outs.completedBytes)
  outs.skippedBytes = prettyBytes(outs.skippedBytes)
  outs.rate = prettyBytes((history[history.length - 1] - history[0]) / history.length) + ' per second'
  logUpdate(JSON.stringify(outs, null, 2))
}

const request = lambda()

const functionName = 'commpFromCarFile'

const clear = () => console.log('\n\n\n\n\n\n')

let mutex

const commp = async (bucket, key) => {
  const opts = { region: 'us-west-2', bucket, key }
  output.inflight += 1
  let commP
  try {
    commP = await request(functionName, opts)
  } catch (e) {
    output.fails += 1
    return null
  }
  output.inflight -= 1
  commP.root = key.slice(0, key.indexOf('/'))
  while (mutex) {
    await mutex
  }
  mutex = putItem(commP)
  await mutex
  mutex = null
  output.completed += 1
  output.completedBytes += commP.size
  return commP
}

const getCarParts = async function * (Bucket) {
  const opts = { Bucket }
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

const run = async argv => {
  let interval
  if (!argv.silent) interval = setInterval(print, 1000)
  const { bucket, concurrency } = argv
  // if (!bucket.startsWith('dumbo-v2-cars-')) throw new Error('bad bucket')
  const limit = limiter(concurrency || 100)
  for await (const { Key } of getCarParts(argv.bucket)) {
    if (!Key.endsWith('.car')) continue
    const item = await getItem({ key: Key, bucket })
    if (!item || argv.force) {
      await limit(commp(bucket, Key))
    } else {
      output.skips += 1
      output.skippedBytes += item.size
    }
  }
  await limit.wait()
  if (interval) clearInterval(interval)
  print()
}

module.exports = run
