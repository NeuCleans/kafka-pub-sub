/// <reference types="node" />
import { Producer, ProduceRequest } from "kafka-node";
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
    static init(defaultTopic?: string, defaultTopicOpts?: KafkaTopicConfig, kHost?: string): Promise<void>;
    static prepareMsgBuffer(data: any, action?: string): Buffer;
    static buildAMessageObject(data: any, toTopic: string, fromTopic?: string, action?: string): Promise<ProduceRequest>;
    static createTopic(topic: string, kafkaTopicConfig?: KafkaTopicConfig): Promise<void>;
    static refreshTopic(topic: string): Promise<void>;
    static send(records: ProduceRequest[]): Promise<void>;
    static close(): void;
}
