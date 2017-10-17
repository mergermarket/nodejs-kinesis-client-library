import { DynamoDBStreams, AWSError } from 'aws-sdk'
import { StreamProvider, GetRecordsData, GetShardIteratorData, Record, EncryptionType } from './stream-provider'

export default function createDynamoDBStreamProvider(client: DynamoDBStreams, stream): StreamProvider {
  return {
    createStream(params, callback) {
      throw new Error('Unsupport operation')
    },

    getRecords(params, callback) {
      throw new Error('Not implemented')
    },

    getShardIterator(params, callback) {
      throw new Error('Not implemented')
    },

    describeStream(params, callback) {
      throw new Error('Not implemented')
    }
  }
}
