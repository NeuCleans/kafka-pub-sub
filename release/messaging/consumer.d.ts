import { Consumer } from 'kafka-node';
export declare class ServiceConsumer {
    static Logger: {
        log: Function;
        error: Function;
    };
    static SERVICE_ID: string;
    private static client;
    private static _client;
    static getClient(): Promise<Consumer>;
    static init(): Promise<void>;
    static subscribe(topic?: string): Promise<void>;
    private static addTopic(topic);
    static listen(cb1?: (message) => any): Promise<void>;
}
