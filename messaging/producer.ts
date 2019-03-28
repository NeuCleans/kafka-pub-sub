// https://medium.com/@theotow/event-sourcing-with-kafka-and-nodejs-9787a8e47716
// https://www.npmjs.com/package/kafka-node
// https://github.com/SOHU-Co/kafka-node
// https://github.com/theotow/nodejs-kafka-example
import { KafkaClient, Producer, ProduceRequest, CreateTopicRequest } from "kafka-node";
import { v4 } from "uuid";

export class ServiceProducer {
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

    static async init(defaultTopic?: string, kHost?: string) {
        // if (this.client && this.isConnected) return;
        this.Logger.log('Init Producer...');

        this._client = new KafkaClient({
            kafkaHost: kHost || process.env.KAFKA_HOST,
            clientId: `${this.clientIdPrefix}_${v4()}`
        });

        this.client = new Producer(
            this._client,
            {
                requireAcks: 1,
                ackTimeoutMs: 100
            }
        );

        await this._onReady();
        this.isConnected = true;
        if (defaultTopic) {
            await this.createTopic(defaultTopic);
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
            partition: 0,
        }
        if (fromTopic) record['key'] = fromTopic; //From
        // console.log(JSON.stringify(record, null, 2));
        return record as ProduceRequest;
    }

    static async createTopic(topic: string) {
        if (!this.client) { await this.init(); }
        try {
            await this._topicExists(topic);
        } catch (error) {
            await this._createTopics(topic);
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
                _self.Logger.error(`Producer:onError - ERROR: ${err.stack}`);
                (cb) ? resolve(cb(err)) : reject(err);
            });
        })
    }

    private static async _onReady() {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self._client.on('ready', async () => {
                _self.Logger.log(`Producer:onReady - Ready...`);
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
                    _self.Logger.log(`Producer:topicExists - Topic Does Not Exist`);
                    reject(err);
                } else { //topic does exist
                    _self.Logger.log(`Producer:topicExists - Topic (${topic}) Already Exists`);
                    resolve();
                }
            });
        })
    }

    private static async _createTopics(topic: string[] | string) {
        topic = Array.isArray(topic) ? topic : [topic];
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self.client.createTopics((topic as string[]), true, (err, data) => {
                if (err) {
                    _self.Logger.error(`Producer:createTopics - ${err.stack}`);
                    reject(err);
                } else {
                    _self.Logger.log(`Producer:createTopics - Topics ${JSON.stringify(data)} created`);
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
                    _self.Logger.log(`Producer:refreshMetadata - Successful`);
                    resolve();
                } else {
                    _self.Logger.error(`Producer:refreshMetadata - ${err.stack}`);
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
                    _self.Logger.error("Producer:send - " + error.stack);
                    reject(error);
                };
                if (data) {
                    _self.Logger.log(`Producer:send - data sent: ${JSON.stringify(data)}`);
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
            _self.Logger.log('Producer:close - Closed');
            if (cb) { cb(); }
        });
    }
};