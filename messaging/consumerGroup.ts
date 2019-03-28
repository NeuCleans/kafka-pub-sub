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
        if (!this.client) { await this.init(); }
        return this.client;
    }

    static async init(defaultTopic: string = 'test', defaultTopicOpts?: KafkaTopicConfig, consumerGroupOpts?: ConsumerGroupOptions) {
        // if (this.client) return;

        ServiceHLProducer.Logger = this.Logger;
        ServiceHLProducer.clientIdPrefix = this.clientIdPrefix;

        await ServiceHLProducer.init(defaultTopic, defaultTopicOpts, consumerGroupOpts.kafkaHost);
        this.Logger.log('Init ConsumerGroup...');

        this._client = new KafkaClient({
            kafkaHost: consumerGroupOpts.kafkaHost || process.env.KAFKA_HOST,
            clientId: `${this.clientIdPrefix}_${v4()}`
        });

        // https://github.com/SOHU-Co/kafka-node#consumergroupoptions-topics
        consumerGroupOpts = (consumerGroupOpts) ? Object.assign({}, defaultKafkaConsumerGroupOpts, consumerGroupOpts) : (defaultKafkaConsumerGroupOpts as any);
        this.client = new ConsumerGroup(consumerGroupOpts, [defaultTopic]); //topics can't be empty

        this.client.client = this._client;

        await this._onReady();
    }

    static async subscribe(topic: string) {
        if (!this.client) { await this.init(); }
        try {
            await this._topicExists(topic);
        } catch (error) { // topic does not exist
            await ServiceHLProducer.createTopic(topic);
        }
        await this._addTopic(topic);
    }

    static async commit(cb?: Function) {
        if (!this.client) { await this.init(); }
        await this._commit();
        if (cb) cb();
    }

    /**
     * @static
     * @param {(message) => any} [cb]
     * @param {boolean} [commit=true] can indicate if you want to handle commits
     * or have the lib commit after every message.  If you set commit to false,
     * you are in charge of calling ServiceConsumer.commit(); likely in your message callback
     * @memberof ServiceConsumerGroup
     */
    static async listen(cb: (message) => any, commit: boolean = true) {
        if (!this.client) { await this.init(); }
        this.Logger.log('ConsumerGroup:listen - listening...')
        await this._onMessage(cb, commit);
    }

    private static async _onMessage(cb: Function, commit: boolean) {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self.client.on('message', async (message) => {
                if (message) {
                    if (message.hasOwnProperty('value') && message.value) message.value = message.value.toString();
                    if (message.hasOwnProperty('key') && message.key) message.key = message.key.toString();
                    _self.Logger.log(`ConsumerGroup:onMessage - Message: ${JSON.stringify(message, null, 2)}`);
                    if (commit) await this._commit();
                    resolve(cb(message))
                }
            });
        })
    }

    static async onError(cb?: Function) {
        if (!this.client) { await this.init(); }
        const _self = this;
        this.Logger.log('ConsumerGroup:onError - listening for errors...')
        return new Promise((resolve, reject) => {
            _self.client.on('error', async (err) => {
                _self.Logger.error(`ConsumerGroup:onError - ERROR: ${err.stack}`);
                (cb) ? resolve(cb(err)) : reject(err);
            });
        })
    }

    private static async _onReady() {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self._client.on('ready', async () => {
                _self.Logger.log(`ConsumerGroup:onReady - Ready...`);
                resolve();
            });
        })
    }

    private static async _addTopic(topic: any) {
        topic = Array.isArray(topic) ? topic : [topic];
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self.client.addTopics(topic, (err, data) => {
                if (err) {
                    _self.Logger.error(`ConsumerGroup:addTopic - ${err.stack}`);
                    reject(err);
                } else { //added
                    _self.Logger.log(`ConsumerGroup:addTopic - Topic: ${JSON.stringify(data)} added`);
                    resolve();
                }
            });
        })
    }

    private static async _topicExists(topic: string) {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self._client.topicExists([topic], (err) => {
                if (err) { //topic does not exist
                    _self.Logger.log(`ConsumerGroup:topicExists - Topic Does Not Exist`);
                    reject(err);
                } else { //topic does exist
                    _self.Logger.log(`ConsumerGroup:topicExists - Topic (${topic}) Already Exists`);
                    resolve();
                }
            });
        })
    }

    private static async _refreshMetadata(topic: string) {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self._client.refreshMetadata([topic], (err) => {
                if (!err) {
                    _self.Logger.log(`ConsumerGroup:refreshMetadata - Successful`);
                    resolve();
                } else {
                    _self.Logger.error(`ConsumerGroup:refreshMetadata - ${err.stack}`);
                    reject(err);
                }
            });
        })
    }

    private static async _commit() {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self.client.commit((err, data) => {
                if (!err) {
                    _self.Logger.log(`ConsumerGroup:commit - ${JSON.stringify(data)}`);
                    resolve();
                } else {
                    _self.Logger.error(`ConsumerGroup:commit - ${err.stack}`);
                    reject(err);
                }
            });
        })
    }
}
