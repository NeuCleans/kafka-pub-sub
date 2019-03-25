import { Producer } from "kafka-node";
export declare class ServiceProducer {
    private static client;
    private static _client;
    static isConnected: boolean;
    static Logger: {
        log: Function;
        error: Function;
    };
    static SERVICE_ID: string;
    static getClient(): Promise<Producer>;
    static init(Logger?: {
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
