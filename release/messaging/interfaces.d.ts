export interface KafkaTopicConfig {
    partitions: number;
    replicationFactor: number;
    configEntries?: {
        name: string;
        value: string;
    }[];
    replicaAssignment?: {
        partition: number;
        replicas: number[];
    }[];
}
export interface Logger {
    log: Function;
    error: Function;
}
