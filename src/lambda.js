const { Lambda } = require('./aws')
const awsConfig = require('aws-config')

class LambdaError extends Error {
  constructor (data, retries) {
    if (!data) return super(data)
    let msg = data.errorMessage
    msg = `\nLambdaError[${data.errorType}]: ` + data.errorMessage
    msg += '\n'
    if (data.stackTrace) {
      msg += data.stackTrace.map(s => '  ' + s).join('\n')
      msg += '\nError[local]: local stack'
    }
    super(msg)
    this.retries = retries
  }
}

const create = (http, profile, region = 'us-west-2') => {
  const _run = async (name, query, retries = 2) => {
    const lambda = new Lambda({ ...awsConfig(), correctCloseSkew: true })
    const FunctionName = name
    const body = http ? { query } : query
    const Payload = Buffer.from(JSON.stringify(body))
    let resp
    try {
      resp = await lambda.invoke({ FunctionName, Payload }).promise()
    } catch (e) {
      console.error(e, '\n\n')
      return _run(name, query, retries - 1)
    }
    if (resp.StatusCode !== 200) throw new Error(`Status not 200, ${resp.StatusCode}`)
    const data = JSON.parse(resp.Payload)
    if (!data) throw new Error(`No response payload for ${name}(${JSON.stringify(query)})`)
    if (data.errorMessage) {
      if (retries > 0 && (
        data.errorMessage.endsWith('Process exited before completing request') ||
        data.errorMessage.includes('We encountered an internal error') ||
        data.errorMessage.includes('SlowDown') || // this sounds bad but it's actually a temporary S3 scaling error
        data.errorMessage.includes('EAI_AGAIN') || // temporary rate limiting on foreign dns queries
        data.errorMessage.endsWith('socket hang up') ||
        data.errorMessage.endsWith('ECONNRESET') ||
        data.errorMessage.endsWith('Unacceptable error code') || // temporary, until landsat finishes
        data.errorMessage.endsWith('exit status 101') || // we see this one with Rust
        data.errorMessage.endsWith('EPROTO'))
      ) {
        return _run(name, query, retries - 1)
      }
      throw new LambdaError(data, retries)
    }
    if (data.headers && data.headers['content-type'].startsWith('application/json')) {
      return JSON.parse(data.body)
    }
    if (!data.body) return data
    return data.body
  }
  return _run
}

module.exports = (...args) => create(true, ...args)
module.exports.raw = (...args) => create(false, ...args)
