import { Producer } from "kafka-node";
export declare class ServiceHLProducer {
    private static client;
    private static _client;
    static isConnected: boolean;
    static Logger: {
        log: Function;
        error: Function;
    };
    static SERVICE_ID: string;
    static kafkaTopicConfig: any;
    static getClient(): Promise<Producer>;
    static init(kafkaTopicConfig?: any, Logger?: {
        log: Function;
        error: Function;
    }, SERVICE_ID?: string): Promise<void>;
    static prepareMsgBuffer(data: {
        id?: string;
    }, action?: string): Buffer;
    static buildAMessageObject(data: any, action?: string, topic?: string): Promise<{}>;
    static createTopic(topic: string): Promise<void>;
    static refreshTopic(topic: string): Promise<void>;
    static send(records: any): Promise<void>;
    static close(): void;
}
