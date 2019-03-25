import { ConsumerGroup } from 'kafka-node';
export declare class ServiceConsumerGroup {
    private static client;
    private static _client;
    static Logger: {
        log: Function;
        error: Function;
    };
    static SERVICE_ID: string;
    static kafkaConsumerGroupOpts: any;
    static getClient(): Promise<ConsumerGroup>;
    static init(kafkaConsumerGroupOpts?: any, Logger?: {
        log: Function;
        error: Function;
    }, SERVICE_ID?: string): Promise<void>;
    static subscribe(topic?: string): Promise<void>;
    private static addTopic(topic);
    static listen(cb1?: (message) => any): Promise<void>;
}
