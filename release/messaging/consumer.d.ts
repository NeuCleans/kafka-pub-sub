import { Consumer } from 'kafka-node';
export declare class ServiceConsumer {
    static Logger: {
        log: Function;
        error: Function;
    };
    static clientIdPrefix: string;
    private static client;
    private static _client;
    static getClient(): Promise<Consumer>;
    static init(defaultTopic?: string, kHost?: string): Promise<void>;
    static subscribe(topic: string): Promise<void>;
    private static addTopic;
    static listen(cb1?: (message: any) => any): Promise<void>;
}
