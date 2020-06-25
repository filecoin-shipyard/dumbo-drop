const AWS = require('aws-sdk')
const awsConfig = require('aws-config')
const doc = new AWS.DynamoDB.DocumentClient(awsConfig())

const waitTime = () => Math.floor(Math.random() * 10000) + 1000
const sleep = () => new Promise(resolve => setTimeout(resolve, waitTime()))

const create = TableName => {
  const mkquery = (keys, valueMap) => {
    const params = {
      ExpressionAttributeNames: {}
    }
    for (const key of keys) {
      params.ExpressionAttributeNames[`#${key}`] = key
    }
    if (valueMap) {
      params.ExpressionAttributeValues = {}
      for (const [key, value] of Object.entries(valueMap)) {
        params.ExpressionAttributeValues[`:${key}`] = value
      }
    }
    return params
  }

  const secret = Math.random()

  const rsleep = ms => new Promise(resolve => setTimeout(() => resolve({ sleep: secret }), ms))

  const retry = async (method, params, retries = 3, timeout = 10000) => {
    let ret
    try {
      ret = await Promise.race([doc[method](params).promise(), rsleep(timeout)])
    } catch (e) {
      console.log(e)
      if (e.retryable) throw e
      if (retries > 0) {
        await sleep()
        return retry(method, params, retries - 1, timeout)
      }
      throw e
    }
    if (ret && ret.sleep === secret) {
      if (retries < 1) {
        throw new Error('Timeout error after retries')
      } else {
        return retry(method, params, retries - 1, timeout)
      }
    }
    return ret
  }

  const ls = async function * (dataset) {
    const params = mkquery(['url', 'dataset'], { dataset, gt: ':' })
    params.KeyConditionExpression = '#url > :gt and #dataset = :dataset'
    params.IndexName = 'dataset-url-noattr'
    params.TableName = TableName

    let resp
    do {
      resp = await retry('query', params)
      const data = resp.Items.map(i => i.url)
      if (resp.LastEvaluatedKey) params.ExclusiveStartKey = resp.LastEvaluatedKey
      yield * data
    } while (resp.LastEvaluatedKey)
  }

  const lsParts = async function * (dataset) {
    const params = mkquery(['url', 'dataset'], { dataset, gt: ':' })
    params.KeyConditionExpression = '#url > :gt and #dataset = :dataset'
    params.IndexName = 'dataset-url-noattr'
    params.TableName = TableName

    let resp
    do {
      resp = await retry('query', params)
      const data = resp.Items.filter(i => !i.split).map(i => i.url)
      if (resp.LastEvaluatedKey) params.ExclusiveStartKey = resp.LastEvaluatedKey
      yield * data
    } while (resp.LastEvaluatedKey)
  }

  const getItem = async (url, attrs) => {
    let Key
    if (typeof url === 'object') Key = url
    else Key = { url }
    const params = { Key, TableName }
    if (attrs) {
      params.AttributesToGet = attrs
    }
    const item = await retry('get', params)
    return item.Item
  }

  const removeAttribute = (url, key) => {
    const params = mkquery([key])
    params.Key = { url }
    params.TableName = TableName
    params.UpdateExpression = `remove #${key}`
    return retry('update', params)
  }

  const update = (url, key, value) => {
    const props = {}
    props[key] = value
    const params = mkquery([key], props)
    params.Key = { url }
    params.TableName = TableName
    params.UpdateExpression = `set #${key} = :${key}`
    return retry('update', params)
  }
  const unset = (url, ...keys) => {
    const params = mkquery(keys)
    params.Key = { url }
    params.TableName = TableName
    params.UpdateExpression = `remove ${keys.map(key => '#' + key).join(', ')}`
    return retry('update', params)
  }
  const updateMany = (url, changes) => {
    const keys = Object.keys(changes)
    const params = mkquery(keys, changes)
    params.Key = { url }
    params.TableName = TableName
    params.UpdateExpression = 'set ' + keys.map(key => `#${key} = :${key}`).join(', ')
    return retry('update', params)
  }

  const bulkUpdate = async (updates, concurrency = 10) => {
    updates = Array.from(updates)
    while (updates.length) {
      const batch = updates.splice(0, concurrency)
      const promises = []
      for (const changes of batch) {
        const { key } = changes
        delete changes.key
        promises.push(updateMany(key, changes))
      }
      await Promise.all(promises)
    }
  }

  const getItems = async (urls, ...attributes) => {
    urls = Array.from(urls)
    const params = { RequestItems: {} }
    const db = new AWS.DynamoDB.DocumentClient({ ...awsConfig(), correctCloseSkew: true })
    const results = {}

    const _get = async urls => {
      params.RequestItems[TableName] = {
        Keys: urls.map(url => ({ url: url })),
        AttributesToGet: ['url', ...attributes]
      }
      const resp = await db.batchGet(params).promise()
      for (const item of resp.Responses[TableName]) {
        if (!item.split) {
          results[item.url] = item
        }
      }
    }

    while (urls.length) {
      await _get(urls.splice(0, 100))
    }
    return results
  }

  const segmentedScan = async function * (segments, query) {
    query.TotalSegments = segments
    const segmenter = async function * (segment) {
      const params = { ...query }
      params.Segment = segment
      let resp
      do {
        resp = await retry('scan', params)
        yield * resp.Items
        if (resp.LastEvaluatedKey) params.ExclusiveStartKey = resp.LastEvaluatedKey
        else return
      } while (resp.LastEvaluatedKey)
    }
    const iters = [...Array(segments).keys()].map(i => segmenter(i))
    const promises = new Set()
    const next = iter => {
      const n = iter.next().then(result => [result, iter])
      n.then(() => promises.delete(n))
      promises.add(n)
    }
    iters.forEach(iter => next(iter))
    while (promises.size) {
      const [result, iter] = await Promise.race(Array.from(promises))
      if (!result.done) {
        yield result.value
        next(iter)
      }
    }
  }

  const _putItems = items => {
    const params = { RequestItems: { } }
    params.RequestItems[TableName] = items.map(item => {
      return { PutRequest: { Item: item } }
    })

    return retry('batchWrite', params)
  }

  const putItems = async items => {
    items = Array.from(items)
    const ret = []
    while (items.length) {
      ret.push(await _putItems(items.splice(0, 10)))
    }
    return [].concat(...ret)
  }
  const del = url => retry('delete', { Key: { url }, TableName })

  const scan = async function * (params) {
    if (!params.TableName) params.TableName = TableName
    let resp
    do {
      resp = await retry('query', params)
      if (resp.LastEvaluatedKey) params.ExclusiveStartKey = resp.LastEvaluatedKey
      yield * resp.Items
    } while (resp.LastEvaluatedKey)
  }
  const slowScan = async function * (params) {
    if (!params.TableName) params.TableName = TableName
    let resp
    do {
      resp = await retry('scan', params)
      const data = resp.Items.map(i => i.url)
      if (resp.LastEvaluatedKey) params.ExclusiveStartKey = resp.LastEvaluatedKey
      yield * resp.Items
    } while (resp.LastEvaluatedKey)
  }
  const exports = {}
  exports.slowScan = slowScan
  exports.ls = ls
  exports.update = update
  exports.getItem = getItem
  exports.mkquery = mkquery
  exports.TableName = TableName
  exports.bulkUpdate = bulkUpdate
  exports.getItems = getItems
  exports.segmentedScan = segmentedScan
  exports.putItems = putItems
  exports.updateMany = updateMany
  exports.removeAttribute = removeAttribute
  exports.lsParts = lsParts
  exports.putItem = item => putItems([item])
  exports.del = del
  exports.retry = retry
  exports.scan = scan
  exports.unset = unset
  return exports
}
module.exports = create
