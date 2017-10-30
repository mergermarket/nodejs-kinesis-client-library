import {StreamProvider} from './stream-provider'
import {createKinesisClient, createDynamoDBStreamsClient} from '../../lib/aws/factory'
import config from '../../lib/config'
import kinesisStreamProviderFactory from './kinesis-stream-provider'
import dynamoDBStreamProviderFactory from './dynamodb-stream-provider'
import {format as formatUrl} from 'url'

export default function createStreamProvider(opts) : StreamProvider {
  switch (opts.streamType) {
    case 'dynamo':
      return createDynamoDBStreamProvider(opts)
    case 'kinesis':
      return createKinesisStreamProvider(opts)
    default:
      throw new Error('Invalid stream type')
  }
}

function createDynamoDBStreamProvider (opts): StreamProvider {
  const client = createDynamoDBStreamsClient(opts.awsConfig, opts.dynamoStreamEndpoint)
  return dynamoDBStreamProviderFactory(client, opts.streamName)
}

function createKinesisStreamProvider (opts): StreamProvider {
  const client = createKinesisClient(opts.awsConfig, getKinesisEndpoint(opts))
  return kinesisStreamProviderFactory(client, opts.streamName)
}

function getKinesisEndpoint(opts) {
  const isLocal = opts.localKinesis
  const port = opts.localKinesisPort
  const customEndpoint = opts.kinesisEndpoint
  let endpoint = null

  if (isLocal) {
    const endpointConfig = config.localKinesisEndpoint
    if (port) {
      endpointConfig.port = port
    }
    endpoint = formatUrl(endpointConfig)
  } else if (customEndpoint) {
    endpoint = customEndpoint
  }

  return endpoint
}
