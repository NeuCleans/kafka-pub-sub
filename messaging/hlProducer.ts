// https://medium.com/@theotow/event-sourcing-with-kafka-and-nodejs-9787a8e47716
// https://www.npmjs.com/package/kafka-node
// https://github.com/SOHU-Co/kafka-node
// https://github.com/theotow/nodejs-kafka-example
import { KafkaClient, Producer, ProduceRequest } from "kafka-node";
import { v4 } from "uuid";
import { defaultKafkaTopicConfig } from "./defaultOpts";
import { KafkaTopicConfig } from "./interfaces";

export class ServiceHLProducer {
    //  ----- can set theses -----
    static Logger: { log: Function, error: Function } = {
        log: (data) => { console.log(data) },
        error: (error) => { console.error(error) }
    };
    static clientIdPrefix: string = "SAMPLE";
    //  ----- can set theses -----

    private static client: Producer;
    private static _client: KafkaClient;
    static isConnected = false;

    static async getClient() {
        if (!this.client) { await this.init(); }
        return this.client;
    }

    static async init(defaultTopic?: string, defaultTopicOpts?: KafkaTopicConfig) {
        const _self = this;

        await new Promise((resolve, reject) => {
            if (_self.isConnected) { resolve(); }

            _self.Logger.log('Init HLProducer...');

            _self._client = new KafkaClient({
                kafkaHost: process.env.KAFKA_HOST || 'localhost:9092',
                clientId: `${_self.clientIdPrefix}_${v4()}`
            });

            _self.client = new Producer(
                _self._client,
                {
                    requireAcks: 1,
                    ackTimeoutMs: 100,
                    partitionerType: 2 //https://github.com/SOHU-Co/kafka-node#producerkafkaclient-options-custompartitioner
                    // https://github.com/SOHU-Co/kafka-node/issues/275#issuecomment-233666209
                });

            _self._client.once('ready', async () => {
                _self.Logger.log('HLProducer:onReady - Ready....');
                _self.isConnected = true;
                if (defaultTopic) await _self.createTopic(defaultTopic, defaultTopicOpts); //create default mailbox
                resolve();
            });

            _self.client.on('error', (err) => {
                _self.Logger.error(`HLProducer:onError - ERROR: ${err.stack}`);
            });
        });
    }

    static prepareMsgBuffer(data: any, action?: string) {
        let jsonData = {
            $ref: v4(),
            timestamp: Date.now(),
            data: data
        }
        if (action) jsonData['action'] = action;
        // if (action) {
        //     jsonData = Object.assign({}, jsonData, { action: action });
        // }
        this.Logger.log("jsonData: " + JSON.stringify(jsonData, null, 2));
        return Buffer.from(JSON.stringify(jsonData));
    }

    static async buildAMessageObject(data: any, toTopic: string, fromTopic?: string, action?: string): Promise<ProduceRequest> {
        if (!this.client) { await this.init(); };
        const _self = this;
        return new Promise<ProduceRequest>((resolve) => {
            const record = {
                topic: toTopic, //To
                messages: _self.prepareMsgBuffer(data, action),
                // key: fromTopic //From
            }
            if (fromTopic) record['key'] = fromTopic; //From
            // return record;
            resolve(record);
        })
    }

    static async createTopic(topic: string, kafkaTopicConfig?: KafkaTopicConfig) {
        if (!this.client) { await this.init(); }
        const _self = this;

        kafkaTopicConfig = (kafkaTopicConfig) ?
            Object.assign({}, defaultKafkaTopicConfig, kafkaTopicConfig) : defaultKafkaTopicConfig
        const topicToCreate = {
            topic,
            ...kafkaTopicConfig
        }

        await new Promise(async (resolve, reject) => {
            const cb = (error, data) => {
                if (error) {
                    _self.Logger.error("HLProducer:createTopic - " + error.stack);
                    reject(error);
                }
                if (data) {
                    _self.Logger.log(`HLProducer:createTopic - Topic created: ${JSON.stringify(data)}`);
                    resolve();
                }
            };

            // if (!this.isConnected) return;
            // console.log("isConnected:", this.isConnected);
            _self._client.createTopics([topicToCreate], cb);
            // this.client.createTopics([topic], cb);
        })
    }

    static async refreshTopic(topic: string) {
        if (!this.client) { await this.init(); }
        // https://github.com/SOHU-Co/kafka-node/issues/676#issuecomment-302401249
        const cb = () => { };
        this._client.refreshMetadata([topic], cb); //cb is not optional here
    }

    /**
     * @static
     * @param {*} records
     * @memberof ServiceProducer
     * @throws Error if Producer Not Yet Connected To Kafka
     */

    static async send(records: ProduceRequest[]) {
        if (!this.client) { await this.init(); }
        // if (!this.isConnected) throw new Error('Producer Not Connected To Kafka');
        const _self = this;

        await new Promise(async (resolve, reject) => {
            const cb = (error, data) => {
                if (error) {
                    _self.Logger.error("HLProducer:send - " + error.stack)
                    reject(error);
                };
                if (data) {
                    _self.Logger.log(`HLProducer:send - data sent: ${JSON.stringify(data)}`);
                    resolve();
                }
            }
            _self.client.send(records, cb);
        });
    }

    static close() {
        if (!this.isConnected) return;
        const _self = this;
        this.client.close(() => {
            _self.isConnected = false;
            _self.Logger.log('HLProducer:close - Closed');
        });
    }
};
