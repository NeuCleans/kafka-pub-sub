/// <reference types="node" />
import { Producer, ProduceRequest } from "kafka-node";
export declare class ServiceProducer {
    static Logger: {
        log: Function;
        error: Function;
    };
    static clientIdPrefix: string;
    private static client;
    private static _client;
    static isConnected: boolean;
    static getClient(): Promise<Producer>;
    static init(defaultTopic?: string, kHost?: string): Promise<void>;
    static prepareMsgBuffer(data: any, action?: string): Buffer;
    static buildAMessageObject(data: any, toTopic: string, fromTopic?: string, action?: string): Promise<ProduceRequest>;
    static createTopic(topic: string): Promise<void>;
    static refreshTopic(topic: string): Promise<void>;
    static send(records: ProduceRequest[]): Promise<void>;
    static close(): void;
}
