import { Kinesis, AWSError } from 'aws-sdk'
import { StreamProvider } from './stream-provider'

export default function createKinesisStreamProvider(client: Kinesis, stream): StreamProvider {
  return {
    createStream(params, callback) {
      client.createStream(params, callback)
    },

    getRecords(params, callback) {
      client.getRecords(params, callback)
    },

    getShardIterator(params, callback) {
      client.getShardIterator(params, callback)
    },

    describeStream(params, callback) {
      client.describeStream(params, callback)
    }
  }
}
