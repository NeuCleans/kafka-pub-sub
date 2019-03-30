import { ConsumerGroup, ConsumerGroupOptions } from 'kafka-node';
import { KafkaTopicConfig, Logger } from './interfaces';
export declare class ServiceConsumerGroup {
    private static Logger;
    private static client;
    private static _client;
    static getClient(): Promise<ConsumerGroup>;
    static init(defaultTopic?: string, defaultTopicOpts?: KafkaTopicConfig, consumerGroupOpts?: ConsumerGroupOptions, clientIdPrefix?: string, logger?: Logger): Promise<void>;
    static subscribe(topic: string): Promise<void>;
    static commit(cb?: Function): Promise<void>;
    static listen(cb: (message: any) => any, commit?: boolean): Promise<void>;
    private static _onMessage;
    static onError(cb?: Function): Promise<{}>;
    private static _onReady;
    private static _addTopic;
    private static _topicExists;
    private static _refreshMetadata;
    private static _commit;
}
