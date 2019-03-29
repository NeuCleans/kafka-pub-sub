// https://medium.com/@theotow/event-sourcing-with-kafka-and-nodejs-9787a8e47716
// https://www.npmjs.com/package/kafka-node
// https://github.com/SOHU-Co/kafka-node
// https://github.com/theotow/nodejs-kafka-example
import { KafkaClient, Producer, ProduceRequest, CreateTopicRequest } from "kafka-node";
import { v4 } from "uuid";
import { defaultKafkaTopicConfig } from "./defaultOpts";
import { KafkaTopicConfig, Logger } from "./interfaces";

export class ServiceHLProducer {
    private static Logger: Logger;
    private static client: Producer;
    private static _client: KafkaClient;
    static isConnected = false;

    static async getClient() {
        if (!this.client) { await this.init(); }
        return this.client;
    }

    static async init(defaultTopic?: string, defaultTopicOpts?: KafkaTopicConfig,
        kHost?: string, clientIdPrefix?: string, logger?: Logger) {

        if (this.client) return;

        this.Logger = (logger) ? logger : {
            log: (data) => { console.log(data) },
            error: (error) => { console.error(error) }
        };

        clientIdPrefix = (clientIdPrefix) ? clientIdPrefix : "TEST";

        this.Logger.log('Init HLProducer...');

        this._client = new KafkaClient({
            kafkaHost: kHost || process.env.KAFKA_HOST,
            clientId: `${clientIdPrefix}_${v4()}`
        });

        this.client = new Producer(
            this._client,
            {
                requireAcks: 1,
                ackTimeoutMs: 100,
                partitionerType: 2 //https://github.com/SOHU-Co/kafka-node#producerkafkaclient-options-custompartitioner
                // https://github.com/SOHU-Co/kafka-node/issues/275#issuecomment-233666209
            });

        await this._onReady();
        this.isConnected = true;
        if (defaultTopic) {
            await this.createTopic(defaultTopic, defaultTopicOpts); //create default mailbox
        }
    }

    static prepareMsgBuffer(data: any, action?: string) {
        let jsonData = {
            $ref: v4(),
            timestamp: Date.now(),
            data: data
        }
        if (action) jsonData['action'] = action;
        // this.Logger.log("jsonData: " + JSON.stringify(jsonData, null, 2));
        return Buffer.from(JSON.stringify(jsonData));
    }

    static async buildAMessageObject(data: any, toTopic: string, fromTopic?: string, action?: string) {
        if (!this.client) { await this.init(); };
        const _self = this;
        const record = {
            topic: toTopic, //To
            messages: _self.prepareMsgBuffer(data, action),
        }
        if (fromTopic) record['key'] = fromTopic; //From
        // console.log(JSON.stringify(record, null, 2));
        return record as ProduceRequest;
    }

    static async createTopic(topic: string, kafkaTopicConfig?: KafkaTopicConfig) {
        if (!this.client) { await this.init(); }
        try {
            await this._topicExists(topic);
        } catch (error) { //need to create topic
            kafkaTopicConfig = (kafkaTopicConfig) ?
                Object.assign({}, defaultKafkaTopicConfig, kafkaTopicConfig) : defaultKafkaTopicConfig

            const topicToCreate = {
                topic,
                ...kafkaTopicConfig
            }
            await this._createTopics([topicToCreate]);
        }
    }

    static async send(records: ProduceRequest | ProduceRequest[]) {
        await this._send(records);
    }

    static async onError(cb?: Function) {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self.client.on('error', async (err) => {
                _self.Logger.error(`HLProducer:onError - ERROR: ${err.stack}`);
                (cb) ? resolve(cb(err)) : reject(err);
            });
        })
    }

    private static async _onReady() {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self._client.on('ready', async () => {
                _self.Logger.log(`HLProducer:onReady - Ready...`);
                resolve();
            });
        })
    }

    private static async _topicExists(topic: string) {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self._client.topicExists([topic], (err) => {
                if (err) { //topic does not exist
                    _self.Logger.log(`HLProducer:topicExists - Topic Does Not Exist`);
                    reject(err);
                } else { //topic does exist
                    _self.Logger.log(`HLProducer:topicExists - Topic (${topic}) Already Exists`);
                    resolve();
                }
            });
        })
    }

    private static async _createTopics(topic: CreateTopicRequest[]) {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self._client.createTopics(topic, async (error, data) => {
                if (error) {
                    _self.Logger.error("HLProducer:createTopic - " + error.stack);
                    reject(error);
                }
                if (data) {
                    _self.Logger.log(`HLProducer:createTopic - Topic created: ${JSON.stringify(data)}`);
                    resolve();
                }
            });
        })
    }

    private static async _refreshMetadata(topic: string) {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            // https://github.com/SOHU-Co/kafka-node/issues/676#issuecomment-302401249
            _self._client.refreshMetadata([topic], (err) => { //cb is not optional here
                if (!err) {
                    _self.Logger.log(`HLProducer:refreshMetadata - Successful`);
                    resolve();
                } else {
                    _self.Logger.error(`HLProducer:refreshMetadata - ${err.stack}`);
                    reject(err);
                }
            });
        })
    }

    /**
     * @static
     * @param {*} records
     * @memberof ServiceProducer
     * @throws Error if Producer Not Yet Connected To Kafka
     */

    private static async _send(data: any) {
        data = Array.isArray(data) ? (data as ProduceRequest[]) : [data];
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self.client.send(data, (error, data) => {
                if (error) {
                    _self.Logger.error("HLProducer:send - " + error.stack);
                    reject(error);
                };
                if (data) {
                    _self.Logger.log(`HLProducer:send - data sent: ${JSON.stringify(data)}`);
                    resolve();
                }
            });
        })
    }

    static close(cb?: Function) {
        if (!this.isConnected) return;
        const _self = this;
        this.client.close(() => {
            _self.isConnected = false;
            _self.Logger.log('HLProducer:close - Closed');
            if (cb) { cb(); }
        });
    }
};