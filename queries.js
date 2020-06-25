const AWS = require('aws-sdk')
const awsConfig = require('aws-config')
const doc = new AWS.DynamoDB.DocumentClient(awsConfig())

// eslint-ignore-next-line
const TableName = 'InitStaging-FilesTable-FRHELBF3T30Q'

const mkquery = (keys, valueMap) => {
  const params = {
    ExpressionAttributeNames: {},
    ExpressionAttributeValues: {}
  }
  for (const key of keys) {
    params.ExpressionAttributeNames[`#${key}`] = key
  }
  for (const [key, value] of Object.entries(valueMap)) {
    params.ExpressionAttributeValues[`:${key}`] = value
  }
  return params
}

const ls = async function * (dataset) {
  const params = mkquery(['url', 'dataset'], { dataset, gt: ':' })
  params.KeyConditionExpression = '#url > :gt and #dataset = :dataset'
  params.IndexName = 'dataset-url-index'
  params.TableName = TableName

  let resp
  do {
    resp = await doc.query(params).promise()
    const data = resp.Items.map(i => i.url)
    if (resp.LastEvaluatedKey) params.ExclusiveStartKey = resp.LastEvaluatedKey
    yield * data
  } while (resp.LastEvaluatedKey)
}

exports.ls = ls
