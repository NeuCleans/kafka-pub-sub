import { Producer } from "kafka-node";
import { KafkaTopicConfig } from "./interfaces";
export declare class ServiceHLProducer {
    static Logger: {
        log: Function;
        error: Function;
    };
    static SERVICE_ID: string;
    private static client;
    private static _client;
    static isConnected: boolean;
    static getClient(): Promise<Producer>;
    static init(): Promise<void>;
    static prepareMsgBuffer(data: {
        id?: string;
    }, action?: string): Buffer;
    static buildAMessageObject(data: any, action?: string, topic?: string): Promise<{}>;
    static createTopic(topic: string, kafkaTopicConfig?: KafkaTopicConfig): Promise<void>;
    static refreshTopic(topic: string): Promise<void>;
    static send(records: any): Promise<void>;
    static close(): void;
}
