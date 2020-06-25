// learn more about HTTP functions here: https://arc.codes/primitives/http
const Block = require('@ipld/block')
const fixed = require('fixed-chunker')
const bent = require('bent')
const get = bent(200, 206)
const createStore = require('./store')
const limiter = require('./limiter')

const parseFile = async (limit, url, headers, retries = 2) => {
  const store = createStore(Block, 'dumbo-v2-block-bucket')
  let stream
  // get a read stream for this file
  try {
    stream = await get(url, null, headers)
  } catch (e) {
    // retry parsing if we get an HTTP 400 Error Code (BAD REQUEST)
    if (e.statusCode > 400) {
      if (!retries) {
        throw new Error(`Unacceptable error code: ${e.statusCode} for ${url}`)
      }
      return parseFile(limit, url, null, retries - 1)
    } else {
      throw e
    }
  }
  const parts = []
  // chunk the file into 1MB buffers, encode them as IPLD blocks
  // and store in S3
  for await (const chunk of fixed(stream, 1024 * 1024)) {
    const block = Block.encoder(chunk, 'raw')
    await limit(store.put(block))
    if (chunk.length) parts.push(block.cid())
  }
  return Promise.all(parts)
}

exports.handler = async (req) => {
  const limit = limiter(100)
  if (req.query.url) {
    // URL for single file - parse it
    const cids = await parseFile(limit, req.query.url, req.query.headers)
    await limit.wait()
    return {
      headers: { 'content-type': 'application/json; charset=utf8' },
      body: JSON.stringify(cids.map(c => c.toString('base64')))
    }
  } else if (req.query.urls) {
    // urls for many files, parse each one in parallel
    const ret = {}
    for (const url of req.query.urls) {
      const cids = await parseFile(limit, url)
      ret[url] = cids.map(c => c.toString('base64'))
    }
    await limit.wait()
    // return parsing status
    return {
      headers: { 'content-type': 'application/json; charset=utf8' },
      body: JSON.stringify(ret)
    }
  }
  throw new Error('Must pass either url or urls in query')
}
