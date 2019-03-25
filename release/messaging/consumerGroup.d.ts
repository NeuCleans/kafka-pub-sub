import { ConsumerGroup } from 'kafka-node';
export declare class ServiceConsumerGroup {
    static Logger: {
        log: Function;
        error: Function;
    };
    static SERVICE_ID: string;
    private static client;
    private static _client;
    static getClient(): Promise<ConsumerGroup>;
    static init(opts?: any): Promise<void>;
    static subscribe(topic?: string): Promise<void>;
    private static addTopic(topic);
    static listen(cb1?: (message) => any): Promise<void>;
}
