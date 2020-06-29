const AWS = require('aws-sdk')
const awsConfig = require('aws-config')
const doc = new AWS.DynamoDB.DocumentClient(awsConfig())
const prettyBytes = require('pretty-bytes')
const bent = require('bent')
// const { PassThrough } = require('stream')
const createParts = require('./create-parts-v2')
const createDB = require('./queries')
const limiter = require('./limiter')
const httpGet = bent()
const httpHead = bent('HEAD')
const ipfsFromCar = require('ipfs-for-car')
const Block = require('@ipld/block')
const CID = require('cids')

const onegig = 1024 * 1024 * 1024
const maxSliceSize = 1024 * 1024 * 912

const ls = db => {
  const attrs = ['carUrl', 'size', 'url', 'split']
  const params = db.mkquery(attrs, { gt: ':' })
  params.FilterExpression = '#url > :gt'
  params.ProjectionExpression = attrs.map(s => '#' + s).join(', ')
  return db.slowScan(params)
}

const run = async argv => {
  const db = createDB(`dumbo-v2-${argv.bucket}`)
  const { update, getItem, getItems } = db
  const limit = limiter(10)
  const { bucket } = argv
  const allocator = { size: 0, entries: 0, splitSize: 0 }
  const carParts = { }
  const carPartUrls = {}
  let partUrls = 0
  let expectedSplitSize = 0
  for await (const { url, size, carUrl, split } of ls(db)) {
    if (argv.showUrls) console.log('url', url)
    if (!url.startsWith('https://') && !url.startsWith('::split::https://')) {
      console.error('Bad URL: ', url)
      if (argv.clean) {
        await limit(doc.delete({ Key: { url }, TableName }).promise())
        continue
      }
    }
    if (carUrl) {
      if (!carParts[carUrl]) carParts[carUrl] = 0
      carParts[carUrl] += size
      partUrls += 1
      if (!carPartUrls[carUrl]) carPartUrls[carUrl] = []
      carPartUrls[carUrl].push(url)
    }
    if (argv.showSize) console.log('size', url, prettyBytes(size))
    if (argv.showItems) console.log({ url, size, carUrl, split })
    if (size < 0) {
      console.error('BAD SIZE: negative', url, size)
    }
    if (url.startsWith(':')) {
      allocator.splitSize += size
      if (size > maxSliceSize) console.error('BAD slice: too large', url)
    } else {
      allocator.size += size
    }
    allocator.entries += 1
    if (size > maxSliceSize) expectedSplitSize += size
  }
  console.log({ allocator })
  console.log('full size', prettyBytes(allocator.size))
  if (expectedSplitSize !== allocator.splitSize) {
    console.error('BAD, SPLIT SIZE DOES NOT MATCH', expectedSplitSize, allocator.splitSize)
  }
  let carPartsTotal = 0
  let carPartsLength = 0
  for (const [part, size] of Object.entries(carParts)) {
    carPartsLength++
    carPartsTotal += size
  }
  if (carPartsTotal !== 0) {
    console.log({ carPartsTotal, carPartsLength })
    console.log('Average car size', prettyBytes(Math.floor(carPartsTotal / carPartsLength)))
  }
  if (carPartsTotal !== allocator.size) {
    console.log('BAD CAR PART ALLOCATOR', carPartsTotal - allocator.size, carPartsTotal, allocator.size)
  }
  let urlsInParts = 0
  for await (const [part, urls] of createParts.getItemsForCARFile(db)) {
    urlsInParts += urls.length
  }
  const _carurl = items => [
    items[Object.keys(items)[0]].carUrl,
    items[Object.keys(items)[0]].root[0]
  ]
  if (argv.checkCarFiles) {
    for (const [carUrl, urls] of Object.entries(carPartUrls)) {
      const items = await getItems(urls, 'root', 'carUrl', 'size', 'parts')
      const [, root] = _carurl(items)
      console.log(`pull ${carUrl}`)
      const { headers } = await httpHead(carUrl)
      const contentLength = parseInt(headers['content-length'])
      if (contentLength > onegig) console.error('BAD car file too large', carUrl)
      const stream = await httpGet(carUrl)
      const ipfs = await ipfsFromCar(stream)
      const rootNode = await ipfs.dag.get(root)
      for (const item of Object.values(items)) {
        const parts = Array.from(item.parts)
        const ref = item.root.slice(0, 2).join('/')
        console.log(`verifying ${ref}`)
        for await (const chunk of ipfs.cat(ref)) {
          const block = Block.encoder(chunk, 'raw')
          const cid = await block.cid()
          const ccid = new CID(parts.shift())
          if (!cid.equals(ccid)) {
            throw new Error("Parts don't match!")
          }
        }
      }
      await ipfs.clearAll()
    }
  }
}

module.exports = run
