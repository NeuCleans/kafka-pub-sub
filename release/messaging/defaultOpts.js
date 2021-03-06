"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const defaultKafkaTopicConfig = {
    partitions: 1,
    replicationFactor: 1,
    configEntries: [
        {
            name: 'compression.type',
            value: 'gzip'
        },
        {
            name: 'min.compaction.lag.ms',
            value: '50'
        }
    ]
};
exports.defaultKafkaTopicConfig = defaultKafkaTopicConfig;
const defaultKafkaConsumerGroupOpts = {
    batch: undefined,
    ssl: true,
    groupId: 'SAMPLE_GROUP_ID',
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    encoding: 'buffer',
    fromOffset: 'latest',
    commitOffsetsOnFirstJoin: true,
    outOfRangeOffset: 'earliest',
    migrateHLC: false,
    migrateRolling: true,
    onRebalance: (isAlreadyMember, callback) => { callback(); }
};
exports.defaultKafkaConsumerGroupOpts = defaultKafkaConsumerGroupOpts;
//# sourceMappingURL=defaultOpts.js.map