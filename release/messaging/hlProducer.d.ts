/// <reference types="node" />
import { Producer, ProduceRequest } from "kafka-node";
import { KafkaTopicConfig, Logger } from "./interfaces";
export declare class ServiceHLProducer {
    private static Logger;
    private static client;
    private static _client;
    static isConnected: boolean;
    static getClient(): Promise<Producer>;
    static init(defaultTopic?: string, defaultTopicOpts?: KafkaTopicConfig, kHost?: string, clientIdPrefix?: string, logger?: Logger): Promise<void>;
    static prepareMsgBuffer(data: any, action?: string): Buffer;
    static buildAMessageObject(data: any, toTopic: string, fromTopic?: string, action?: string): Promise<ProduceRequest>;
    static createTopic(topic: string, kafkaTopicConfig?: KafkaTopicConfig): Promise<void>;
    static send(records: ProduceRequest | ProduceRequest[]): Promise<void>;
    static onError(cb?: Function): Promise<{}>;
    private static _onReady;
    private static _topicExists;
    private static _createTopics;
    private static _refreshMetadata;
    private static _send;
    static close(cb?: Function): void;
}
