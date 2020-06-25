const AWS = require('aws-sdk')
var https = require('https')
var agent = new https.Agent({
  maxSockets: 5000
})

AWS.config.update({ httpOptions: { agent } })
module.exports = AWS
