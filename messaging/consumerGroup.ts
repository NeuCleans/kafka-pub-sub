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
        if (this.client) return;
        const _self = this;

        await new Promise<void>(async (resolve, reject) => {
            ServiceHLProducer.Logger = _self.Logger;
            ServiceHLProducer.clientIdPrefix = _self.clientIdPrefix;

            await ServiceHLProducer.init(defaultTopic, defaultTopicOpts, consumerGroupOpts.kafkaHost);
            // .then(() => {
            _self.Logger.log('Init ConsumerGroup...');

            _self._client = new KafkaClient({
                kafkaHost: consumerGroupOpts.kafkaHost || process.env.KAFKA_HOST,
                clientId: `${_self.clientIdPrefix}_${v4()}`
            });

            // https://github.com/SOHU-Co/kafka-node#consumergroupoptions-topics
            consumerGroupOpts = (consumerGroupOpts) ? Object.assign({}, defaultKafkaConsumerGroupOpts, consumerGroupOpts) : (defaultKafkaConsumerGroupOpts as any);
            _self.client = new ConsumerGroup(consumerGroupOpts, ['test']); //topics can't be empty

            _self.client.client = _self._client;

            _self._client.on('ready', async () => {
                // setTimeout(async () => {
                _self.Logger.log(`ConsumerGroup:onReady - Ready...`);
                await _self.subscribe(defaultTopic);
                resolve();
                // }, 5 * 1000);
            });

            _self.client.on('error', (err) => {
                _self.Logger.error(`ConsumerGroup:onError - ERROR: ${err.stack}`);
            });
        }); //need to make sure Producer creates default topic
        // });
    }

    static async subscribe(topic: string) {
        if (!this.client) { throw new Error("ConsumerGroup client not initialized. Please call init(<topic>) first") }
        const _self = this;

        await new Promise(async (resolve, reject) => {
            const cb = async (err) => {
                if (err) { //topic already exists
                    await _self.addTopic(topic);
                    resolve()
                } else {
                    await ServiceHLProducer.createTopic(topic);
                    await _self.addTopic(topic)
                    resolve();
                }
            }
            _self._client.topicExists([topic], cb);
        })
    }

    private static async addTopic(topic: string) {
        if (!this.client) { throw new Error("ConsumerGroup client not initialized. Please call init(<topic>) first") }
        const _self = this;

        return new Promise(async (resolve, reject) => {
            const cb = (err, data) => {
                if (err) {
                    _self.Logger.error(`ConsumerGroup:addTopic - ${err.stack}`);
                    reject(err);
                }
                if (data) {
                    _self.Logger.log(`ConsumerGroup:addTopic - Topic: ${JSON.stringify(data)} added`);
                    resolve();
                }
            };

            try {
                // https://github.com/SOHU-Co/kafka-node/issues/781#issuecomment-336154404
                // await _self.refreshMetadata(topic);
                await _self.client.addTopics([topic], cb); // only works w/ string[] not Topic[] and must refreshMetadata
            } catch (error) {
                if (error.stack.indexOf('LeaderNotAvailable') > -1) {
                    _self.Logger.log("ConsumerGroup:refreshMetadata - LeaderNotAvailable...Retrying...");
                    await _self.addTopic(topic);
                } else {
                    _self.Logger.error("ConsumerGroup:refreshMetadata - " + error.stack);
                    _self.Logger.error("ConsumerGroup:addTopics - TOPIC NOT ADDED: " + topic)
                }
            }
        })
    }

    static async refreshMetadata(topic: string) {
        if (!this.client) { throw new Error("ConsumerGroup client not initialized. Please call init(<topic>) first") }
        const _self = this;

        return new Promise((resolve, reject) => {
            _self._client.refreshMetadata([topic], async (err) => {
                if (err) reject(err);
                if (!err) resolve();
            });
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
                    if (message.hasOwnProperty('value') && message.value) message.value = message.value.toString();
                    if (message.hasOwnProperty('key') && message.key) message.key = message.key.toString();
                    return ((cb1) ? cb1(message) : message);
                }
            });
        });
    }
}
