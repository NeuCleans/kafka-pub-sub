// https://stackoverflow.com/a/37701524/3562407
// https://bertrandszoghy.wordpress.com/2017/06/27/nodejs-querying-messages-in-apache-kafka/
// https://stackoverflow.com/a/42579505/3562407 - consumergroups

import { KafkaClient, ConsumerGroup, ConsumerGroupOptions } from 'kafka-node';
import { v4 } from "uuid";
import { ServiceHLProducer } from './hlProducer';
import { defaultKafkaConsumerGroupOpts } from './defaultOpts';
import { KafkaTopicConfig } from './interfaces';

export class ServiceConsumerGroup {
    //  ----- can set theses -----
    static Logger: { log: Function, error: Function } = {
        log: (data) => { console.log(data) },
        error: (error) => { console.error(error) }
    };
    static clientIdPrefix: string = "SAMPLE";
    //  ----- can set theses -----

    private static client: ConsumerGroup;
    private static _client: KafkaClient;

    static async getClient() {
        if (!this.client) { throw new Error("ConsumerGroup client not initialized. Please call init(<topic>) first") }
        return this.client;
    }

    static async init(defaultTopic: string, defaultTopicOpts?: KafkaTopicConfig, consumerGroupOpts?: ConsumerGroupOptions) {
        // https://github.com/SOHU-Co/kafka-node#consumergroupoptions-topics
        consumerGroupOpts = (consumerGroupOpts) ? Object.assign({}, defaultKafkaConsumerGroupOpts, consumerGroupOpts) : (defaultKafkaConsumerGroupOpts as any);
        const _self = this;

        await new Promise(async (resolve, reject) => {
            ServiceHLProducer.Logger = _self.Logger;
            ServiceHLProducer.clientIdPrefix = _self.clientIdPrefix;

            await ServiceHLProducer.init(defaultTopic, defaultTopicOpts)
                .then(() => {
                    _self.Logger.log('Init ConsumerGroup...');

                    const kHost = process.env.KAFKA_HOST || 'localhost:9092';

                    _self._client = new KafkaClient({
                        kafkaHost: kHost,
                        clientId: `${_self.clientIdPrefix}_${v4()}`
                    });

                    const options = Object.assign({}, consumerGroupOpts, { kafkaHost: kHost });
                    _self.client = new ConsumerGroup(options, [defaultTopic]) //topics can't be empty
                    //subscribe to service topic
                    _self.client.client = _self._client;

                    _self._client.once('ready', () => {
                        _self.Logger.log(`ConsumerGroup:onReady - Ready...`);
                        resolve();
                    });

                    _self.client.on('error', (err) => {
                        _self.Logger.error(`ConsumerGroup:onError - ERROR: ${err.stack}`);
                    });
                }); //need to make sure Producer creates default topic
        });
    }

    static async subscribe(topic: string) {
        if (!this.client) { throw new Error("ConsumerGroup client not initialized. Please call init(<topic>) first") }
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
        if (!this.client) { throw new Error("ConsumerGroup client not initialized. Please call init(<topic>) first") }
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
    //     if (!this.client) { throw new Error("ConsumerGroup client not initialized. Please call init(<topic>) first") }
    //     const _self = this;
    //     const cb = (err, data) => {
    //         this.Logger.log('ConsumerGroup:commit - Committing...');
    //         if (err) { this.Logger.log(`ConsumerGroup:commit - Error: ${err.stack}`); }
    //         if (data) {
    //             this.Logger.log(`ConsumerGroup:commit - Data: ${JSON.stringify(data)}`);
    //             // return (cb1) ? cb1(message) : message;
    //         }
    //     };
    //     this.client.commit(cb);
    // }

    static async listen(cb1?: (message) => any) {
        if (!this.client) { throw new Error("ConsumerGroup client not initialized. Please call init(<topic>) first") }
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
