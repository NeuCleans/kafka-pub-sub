import { ConsumerGroup, ConsumerGroupOptions } from 'kafka-node';
import { KafkaTopicConfig } from './interfaces';
export declare class ServiceConsumerGroup {
    static Logger: {
        log: Function;
        error: Function;
    };
    static clientIdPrefix: string;
    private static client;
    private static _client;
    static getClient(): Promise<ConsumerGroup>;
    static init(defaultTopic?: string, defaultTopicOpts?: KafkaTopicConfig, consumerGroupOpts?: ConsumerGroupOptions): Promise<void>;
    static subscribe(topic: string): Promise<void>;
    private static addTopic;
    static listen(cb1?: (message: any) => any): Promise<void>;
}
