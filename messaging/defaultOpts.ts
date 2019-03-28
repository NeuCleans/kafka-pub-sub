const defaultKafkaTopicConfig = {
    // https://github.com/SOHU-Co/kafka-node/blob/42d61e943751238b38789cb4fe1ef5d0865c63aa/README.md#createtopicstopics-cb
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
}

const defaultKafkaConsumerGroupOpts = {
    // https://github.com/SOHU-Co/kafka-node#consumergroupoptions-topics
    batch: undefined, // put client batch settings if you need them
    ssl: true, // optional (defaults to false) or tls options hash
    groupId: 'SAMPLE_GROUP_ID',
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    encoding: 'buffer', // default is utf8, use 'buffer' for binary data
    fromOffset: 'latest', // default
    commitOffsetsOnFirstJoin: true, // on the very first time this consumer group subscribes to a topic, record the offset returned in fromOffset (latest/earliest)
    outOfRangeOffset: 'earliest', // default
    migrateHLC: false,    // for details please see Migration section below
    migrateRolling: true,
    onRebalance: (isAlreadyMember, callback) => { callback(); } // or null
};

export {
    defaultKafkaConsumerGroupOpts,
    defaultKafkaTopicConfig
}