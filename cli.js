#!/usr/bin/env node
const parseBucketV2 = require('./src/parse-bucket-v2')
const createPartsV2 = require('./src/create-parts-v2')
const commp = require('./src/commp')
const inspect = require('./src/inspect')

const runPullBucketV2 = async argv => {
  await parseBucketV2(argv.bucket, argv.prefix, argv.start, argv.concurrency, argv.checkHead, argv.force)
  console.log('all done :)')
  process.exit(0) // HACK: force process to exit because it doesn't right now...
}

const bucketOptions = yargs => {
  yargs.option('concurrency', {
    desc: 'Concurrent Lambda requests',
    default: 100
  })
  yargs.option('checkHead', {
    desc: 'Perform HEAD request to verify access to every URL',
    default: false,
    type: 'boolean'
  })
  yargs.option('force', {
    desc: 'Overwrite existing data instead of skipping',
    default: false,
    type: 'boolean'
  })
}

const createParts2Options = yargs => {
  yargs.option('concurrency', {
    desc: 'Concurrent Lambda requests',
    default: 100
  })
}

const inspectOptions = yargs => {
  yargs.option('clean', {
    desc: 'Clean known bad data',
    boolean: true,
    default: false
  })
  yargs.option('checkCarFiles', {
    desc: 'Pull down car files and validate their contents',
    boolean: true,
    default: false
  })
  yargs.option('showItems', {
    desc: 'Print every item',
    boolean: true,
    default: false
  })
  yargs.option('showUrls', {
    desc: 'Print every URL',
    boolean: true,
    default: false
  })
}

const commpOptions = yargs => {
  bucketOptions(yargs)
  yargs.option('concurrency', {
    desc: 'Concurrent Lambda requests',
    default: 300
  })
  yargs.option('force', {
    desc: 'Overwrite existing car files if they exist',
    boolean: true,
    default: false
  })
  yargs.option('silent', {
    desc: 'Suppress realtime info',
    boolean: true,
    default: false
  })
}

const yargs = require('yargs')
// eslint-disable-next-line
const args = yargs
  // .command('pull-bucket <bucket> [prefix]', 'Load all the contents of a bucket', bucketOptions, runPullBucket)
  .command('pull-bucket-v2 <bucket> [prefix]', 'Parse and store bucket in unique table', bucketOptions, runPullBucketV2)
  // .command('pull-file <bucket> <key>', 'Pull and parse one file', () => {}, runPullFile)
  // .command('allocate <bucket>', 'Allocate all the data for a parsed bucket to parts', () => {}, runAllocator)
  // .command('create-parts <bucket>', 'Create car files for each one gig data part', createPartsOptions, runCreateParts)
  .command('create-parts-v2 <bucket>', 'Create car files for each one gig data part', createParts2Options, createPartsV2)
  .command('inspect <bucket>', 'Inspect data about each entry for the bucket', inspectOptions, inspect)
  .command('commp <bucket>', 'Calculate and store commp for the CAR files in a bucket', commpOptions, commp)
  // .command('pluck <url>', 'Remove all entries for URL', pluckOptions, pluck)
  .argv
