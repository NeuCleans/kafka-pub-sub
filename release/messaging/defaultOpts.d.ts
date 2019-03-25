declare const defaultKafkaTopicConfig: {
    partitions: number;
    replicationFactor: number;
    configEntries: {
        name: string;
        value: string;
    }[];
};
declare const defaultKafkaConsumerGroupOpts: {
    batch: any;
    ssl: boolean;
    groupId: string;
    sessionTimeout: number;
    protocol: string[];
    encoding: string;
    fromOffset: string;
    commitOffsetsOnFirstJoin: boolean;
    outOfRangeOffset: string;
    migrateHLC: boolean;
    migrateRolling: boolean;
    onRebalance: (isAlreadyMember: any, callback: any) => void;
};
export { defaultKafkaConsumerGroupOpts, defaultKafkaTopicConfig };
