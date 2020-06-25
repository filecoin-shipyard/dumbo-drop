// learn more about HTTP functions here: https://arc.codes/primitives/http
const AWS = require('aws-sdk')
const Block = require('@ipld/block')
const importer = require('unixfsv1-part-importer')
const CarDatastore = require('datastore-car')
const awsConfig = require('aws-config')
const s3 = new AWS.S3(awsConfig())
const s3stream = require('s3-upload-stream')(s3)
const CID = require('cids')
const createStore = require('./store')

const createGetBlock = async (cacheBlocks, blockBucket) => {
  const cache = new Map()
  const cids = await Promise.all(cacheBlocks.map(b => b.cid()))
  for (const cid of cids) {
    cache.set(cid.toString('base32'), cacheBlocks.shift())
  }
  const { get } = createStore(Block, blockBucket)
  const getBlock = async cid => {
    const key = cid.toString('base32')
    if (cache.has(key)) return cache.get(key)
    return get(cid)
  }
  return getBlock
}

const onemeg = 1024 * 1024

exports.handler = async (req) => {
  if (!req.query.Bucket || !req.query.blockBucket || !req.query.files) {
    throw new Error('Missing required arguments')
  }
  const { files } = req.query
  const ret = { }
  const urls = {}
  const roots = []
  const dagBlocks = []
  for (let [filename, [_parts, size]] of Object.entries(files)) {
    const parts = []
    for (const cid of _parts) {
      const _size = size < onemeg ? size : onemeg
      size = size - _size
      parts.push({ cidVersion: 1, cid: new CID(cid), size: _size })
    }
    urls[filename] = parts.map(x => x.cid)
    dagBlocks.push(...await importer(parts))
    const root = await dagBlocks[dagBlocks.length - 1].cid()
    roots.push(root)
    ret[filename] = [roots.length - 1, root.toString('base32')]
  }

  const rootBlock = Block.encoder(roots, 'dag-cbor')
  dagBlocks.push(rootBlock)

  const carRoot = await rootBlock.cid()
  const rootString = carRoot.toString('base32')
  const carFilename = `${rootString}/${rootString}.car`
  const getBlock = await createGetBlock(dagBlocks, req.query.blockBucket)
  const opts = {
    ACL: 'public-read',
    Bucket: req.query.Bucket,
    Key: carFilename
  }
  const upload = s3stream.upload(opts)
  const uploaded = new Promise(resolve => upload.once('uploaded', resolve))
  const writer = await CarDatastore.writeStream(upload)
  await CarDatastore.completeGraph(carRoot, getBlock, writer, 1000)

  const details = await uploaded

  return {
    headers: { 'content-type': 'application/json; charset=utf8' },
    body: JSON.stringify({ details, results: ret, root: carRoot.toString('base32'), carFilename })
  }
}
