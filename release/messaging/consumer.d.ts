import { Consumer } from 'kafka-node';
export declare class ServiceConsumer {
    private static client;
    private static _client;
    static Logger: {
        log: Function;
        error: Function;
    };
    static SERVICE_ID: string;
    static getClient(): Promise<Consumer>;
    static init(Logger?: {
        log: Function;
        error: Function;
    }, SERVICE_ID?: string): Promise<void>;
    static subscribe(topic?: string): Promise<void>;
    private static addTopic(topic);
    static listen(cb1?: (message) => any): Promise<void>;
}
