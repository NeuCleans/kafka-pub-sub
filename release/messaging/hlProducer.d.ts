/// <reference types="node" />
import { Producer } from "kafka-node";
import { KafkaTopicConfig } from "./interfaces";
export declare class ServiceHLProducer {
    static Logger: {
        log: Function;
        error: Function;
    };
    static clientIdPrefix: string;
    private static client;
    private static _client;
    static isConnected: boolean;
    static getClient(): Promise<Producer>;
    static init(defaultTopic?: string, defaultTopicOpts?: KafkaTopicConfig): Promise<void>;
    static prepareMsgBuffer(data: any, action?: string): Buffer;
    static buildAMessageObject(data: any, toTopic: string, action?: string, fromTopic?: string): Promise<{}>;
    static createTopic(topic: string, kafkaTopicConfig?: KafkaTopicConfig): Promise<void>;
    static refreshTopic(topic: string): Promise<void>;
    static send(records: any): Promise<void>;
    static close(): void;
}
