// https://stackoverflow.com/a/37701524/3562407
// https://bertrandszoghy.wordpress.com/2017/06/27/nodejs-querying-messages-in-apache-kafka/
// https://stackoverflow.com/a/42579505/3562407 - consumergroups

import { KafkaClient, Consumer } from 'kafka-node';
import { ServiceProducer } from './producer';
import { v4 } from "uuid";

export class ServiceConsumer {
    //  ----- can set theses -----
    static Logger: { log: Function, error: Function } = {
        log: (data) => { console.log(data) },
        error: (error) => { console.error(error) }
    };
    static clientIdPrefix: string = "SAMPLE";
    //  ----- can set theses -----

    private static client: Consumer;
    private static _client: KafkaClient;

    static async getClient() {
        if (!this.client) { await this.init(); }
        return this.client;
    }

    static async init(defaultTopic?: string, kHost?: string) {
        // if (this.client) return;

        ServiceProducer.Logger = this.Logger;
        ServiceProducer.clientIdPrefix = this.clientIdPrefix;

        await ServiceProducer.init(defaultTopic, kHost);
        this.Logger.log('Init Consumer...');

        this._client = new KafkaClient({
            kafkaHost: kHost || process.env.KAFKA_HOST,
            clientId: `${this.clientIdPrefix}_${v4()}`,
            // requestTimeout: 9999999
        });
        this.client = new Consumer(
            this._client,
            [],
            {
                autoCommit: false,
                fromOffset: true
            }
        );

        await this._onReady();
        if (defaultTopic) await this.subscribe(defaultTopic);
    }

    static async subscribe(topic: string) {
        if (!this.client) { await this.init(); }
        try {
            await this._topicExists(topic);
        } catch (error) { // topic does not exist
            await ServiceProducer.createTopic(topic);
        }
        await this._addTopic(topic);
        //start reading topic from where client left off
        // await _self.addTopic([{ topic: topic, partition: 0, offset: 0 }]);
    }

    static async commit(cb?: Function) {
        if (!this.client) { await this.init(); }
        await this._commit();
        if (cb) cb();
    }

    /**
     *
     *
     * @static
     * @param {(message) => any} cb
     * @param {boolean} [commit=true]
     * If you set commit to false, you are in charge of calling ServiceConsumer.commit(); likely in your message callback
     * @memberof ServiceConsumer
     */
    static async listen(cb: (message) => any, commit: boolean = true) {
        if (!this.client) { await this.init(); }
        this.Logger.log('Consumer:listen - listening...')
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
                    _self.Logger.log(`Consumer:onMessage - Message: ${JSON.stringify(message, null, 2)}`);
                    if (commit) await this._commit();
                    resolve(cb(message));
                }
            });
        })
    }

    static async onError(cb?: Function) {
        if (!this.client) { await this.init(); }
        const _self = this;
        this.Logger.log('Consumer:onError - listening for errors...')
        return new Promise((resolve, reject) => {
            _self.client.on('error', async (err) => {
                _self.Logger.error(`Consumer:onError - ERROR: ${err.stack}`);
                (cb) ? resolve(cb(err)) : reject(err);
            });
        })
    }

    private static async _onReady() {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self._client.on('ready', async () => {
                _self.Logger.log(`Consumer:onReady - Ready...`);
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
                    _self.Logger.error(`Consumer:addTopic - ${err.stack}`);
                    reject(err);
                } else { //added
                    _self.Logger.log(`Consumer:addTopic - Topic: ${JSON.stringify(data)} added`);
                    resolve();
                }
            }, false);
        })
    }

    private static async _topicExists(topic: string) {
        if (!this.client) { await this.init(); }
        const _self = this;
        return new Promise((resolve, reject) => {
            _self._client.topicExists([topic], (err) => {
                if (err) { //topic does not exist
                    _self.Logger.log(`Consumer:topicExists - Topic Does Not Exist`);
                    reject(err);
                } else { //topic does exist
                    _self.Logger.log(`Consumer:topicExists - Topic (${topic}) Already Exists`);
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
                    _self.Logger.log(`Consumer:refreshMetadata - Successful`);
                    resolve();
                } else {
                    _self.Logger.error(`Consumer:refreshMetadata - ${err.stack}`);
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
                    _self.Logger.log(`Consumer:commit - ${JSON.stringify(data)}`);
                    resolve();
                } else {
                    _self.Logger.error(`Consumer:commit - ${err.stack}`);
                    reject(err);
                }
            });
        })
    }
}
