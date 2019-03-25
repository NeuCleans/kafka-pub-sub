// https://stackoverflow.com/a/37701524/3562407
// https://bertrandszoghy.wordpress.com/2017/06/27/nodejs-querying-messages-in-apache-kafka/
// https://stackoverflow.com/a/42579505/3562407 - consumergroups

import { KafkaClient, ConsumerGroup } from 'kafka-node';
import { v4 } from "uuid";
import { ServiceHLProducer } from './hlProducer';
import { defaultKafkaConsumerGroupOpts } from './defaultOpts';

export class ServiceConsumerGroup {
    private static client: ConsumerGroup;
    private static _client: KafkaClient;
    static Logger: { log: Function, error: Function };
    static SERVICE_ID: string;
    static kafkaConsumerGroupOpts: any;

    static async getClient() {
        if (!this.client) { await this.init(); }
        return this.client;
    }

    static async init(kafkaConsumerGroupOpts?: any, Logger?: { log: Function, error: Function }, SERVICE_ID?: string) {
        this.kafkaConsumerGroupOpts = kafkaConsumerGroupOpts || defaultKafkaConsumerGroupOpts;
        this.Logger = Logger || {
            log: (data) => { console.log(data) },
            error: (error) => { console.error(error) }
        };
        this.SERVICE_ID = SERVICE_ID || v4();

        const _self = this;
        await new Promise(async (resolve, reject) => {
            await ServiceHLProducer.init()
                .then(() => {
                    _self.Logger.log('Init ConsumerGroup...');

                    _self._client = new KafkaClient({
                        kafkaHost: process.env.KAFKA_HOST,
                        clientId: _self.SERVICE_ID
                    });
                    // https://github.com/SOHU-Co/kafka-node#consumergroupoptions-topics
                    const options = Object.assign({}, (kafkaConsumerGroupOpts as any),
                        { kafkaHost: process.env.KAFKA_HOST });
                    _self.client = new ConsumerGroup(options, [_self.SERVICE_ID]) //topics can't be empty
                    //subscribe to service topic
                    _self.client.client = _self._client;

                    _self._client.on('ready', () => {
                        _self.Logger.log(`ConsumerGroup:onReady - Ready...`);
                        resolve();
                    });

                    _self.client.on('error', (err) => {
                        _self.Logger.error(`ConsumerGroup:onError - ERROR: ${err.stack}`);
                    });
                }); //need to make sure Producer creates default topic
        });
    }

    static async subscribe(topic: string = this.SERVICE_ID) {
        if (!this.client) { await this.init(); }
        const _self = this;

        await new Promise(async (resolve, reject) => {
            const cb = (err) => {
                if (!err) {
                    resolve(_self.addTopic(topic));
                } else {
                    ServiceHLProducer.createTopic(topic)
                        .then(() => {
                            resolve(_self.addTopic(topic));
                        });
                }
            }
            this._client.topicExists([topic], cb);
        })
    }

    private static async addTopic(topic: string) {
        if (!this.client) { await this.init(); }
        const _self = this;

        const cb = (err, data) => {
            if (err) {
                _self.Logger.error(`ConsumerGroup:addTopic - ${err.stack}`);
            }
            if (data) _self.Logger.log(`ConsumerGroup:addTopic - Topic: ${JSON.stringify(data)} added`);
        };

        // https://github.com/SOHU-Co/kafka-node/issues/781#issuecomment-336154404
        this._client.refreshMetadata([topic], (err) => {
            if (!err) {
                _self.client.addTopics([topic], cb); // only works w/ string[] not Topic[] and must refreshMetadata
            }
        });
    }

    // static async commit() {
    //     if (!this.client) { await this.init(); }
    //     const cb = (err, data) => {
    //         Logger.log('ConsumerGroup:commit - Committing...');
    //         if (err) { Logger.log(`ConsumerGroup:commit - Error: ${err.stack}`); }
    //         if (data) {
    //             Logger.log(`ConsumerGroup:commit - Data: ${JSON.stringify(data)}`);
    //             // return (cb1) ? cb1(message) : message;
    //         }
    //     };
    //     this.client.commit(cb);
    // }

    static async listen(cb1?: (message) => any) {
        if (!this.client) { await this.init(); }
        const _self = this;
        this.client.on('message', (message) => {
            this.client.commit((err, data) => {
                _self.Logger.log('ConsumerGroup:onMessage - Committing...');
                if (err) { _self.Logger.log(`ConsumerGroup:onMessage - Error: ${err.stack}`); }
                if (data) {
                    _self.Logger.log(`ConsumerGroup:onMessage - Data: ${JSON.stringify(data)}`);
                    message.value = message.value.toString();
                    message.key = message.key.toString();
                    return ((cb1) ? cb1(message) : message);
                }
            });
        });
    }
}
