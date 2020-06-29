const AWS = require('aws-sdk')
const awsConfig = require('aws-config')

const s3 = new AWS.S3(awsConfig())

const encodeKey = cid => {
  const key = cid.toString('base32')
  const full = `${key}/encode`
  return full
}
// IPLD block store for S3
module.exports = (Block, Bucket, ACL = 'public-read') => {
  const put = async block => {
    const params = {
      ACL,
      Bucket,
      Body: block.encode(),
      Key: await block.cid().then(cid => encodeKey(cid))
    }
    return s3.putObject(params).promise()
  }
  const get = async cid => {
    const params = { Bucket, Key: encodeKey(cid) }
    const data = await s3.getObject(params).promise()
    return Block.create(data, cid)
  }
  return { put, get }
}
