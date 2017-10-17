import {StreamProvider} from './stream-provider'
import {createKinesisClient, createDynamoDBStreamsClient} from '../../lib/aws/factory'
import config from '../../lib/config'
import kinesisStreamProviderFactory from './kinesis-stream-provider'
import dynamoStreamProviderFactory from './dynamo-stream-provider'
import {format as formatUrl} from 'url'

export default function createStreamProvider(opts) : StreamProvider {
  switch (opts.streamType) {
    case 'dynamo':
      return createDynamoStreamProvider(opts)
    case 'kinesis':
      return createKinesisStreamProvider(opts)
    default:
      throw new Error('Invalid stream type')
  }
}

function createDynamoStreamProvider (opts): StreamProvider {
  if (!opts.dynamoStreamEndpoint) {
    throw new Error('dynamo-stream-endpoint must be set when using dynamo streams')
  }
  const dynamoStreamClient = createDynamoDBStreamsClient(opts, opts.dynamoStreamEndpoint)
  return dynamoStreamProviderFactory(dynamoStreamClient, opts.streamName)
}

function createKinesisStreamProvider (opts): StreamProvider {
  const kinesis = createKinesisClient(opts.awsConfig, getKinesisEndpoint(opts))
  return kinesisStreamProviderFactory(kinesis, opts.streamName)
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
