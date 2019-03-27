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

    static async init(defaultTopic?: string) {
        const _self = this;
        await new Promise(async (resolve, reject) => {
            ServiceProducer.Logger = _self.Logger;
            ServiceProducer.clientIdPrefix = _self.clientIdPrefix;

            await ServiceProducer.init(defaultTopic)
                .then(() => {
                    _self.Logger.log('Init Consumer...');

                    _self._client = new KafkaClient({
                        kafkaHost: process.env.KAFKA_HOST || 'localhost:9092',
                        clientId: `${_self.clientIdPrefix}_${v4()}`
                    });
                    _self.client = new Consumer(
                        _self._client,
                        [],
                        {
                            autoCommit: false,
                            fromOffset: true
                        }
                    );

                    _self._client.once('ready', async () => {
                        _self.Logger.log(`Consumer:onReady - Ready...`);
                        if (defaultTopic) await _self.subscribe(defaultTopic);
                        resolve();
                    });

                    _self.client.on('error', (err) => {
                        _self.Logger.error(`Consumer:onError - ERROR: ${err.stack}`);
                    });
                });
        });
    }

    static async subscribe(topic: string) {
        if (!this.client) { await this.init(); }
        const _self = this;

        await new Promise(async (resolve, reject) => {
            const cb = (err) => {
                if (!err) {
                    resolve(_self.addTopic(topic));
                } else {
                    ServiceProducer.createTopic(topic)
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
                _self.Logger.error(`Consumer:addTopic - ${err.stack}`);
            }
            if (data) _self.Logger.log(`Consumer:addTopic - Topic: ${JSON.stringify(data)} added`);
        };

        this._client.refreshMetadata([topic], (err) => {
            if (!err) {
                //start reading topic from where client left off
                _self.client.addTopics([{ topic: topic, partition: 0, offset: 0 }], cb);
            }
        });
    }

    // static async commit() {
    //     if (!this.client) { await this.init(); }
    //     const _self = this;
    //     const cb = (err, data) => {
    //         this.Logger.log('Consumer:commit - Committing...');
    //         if (err) { this.Logger.log(`Consumer:commit - Error: ${err.stack}`); }
    //         if (data) {
    //             this.Logger.log(`Consumer:commit - Data: ${JSON.stringify(data)}`);
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
                _self.Logger.log('Consumer:onMessage - Committing...');
                if (err) { _self.Logger.log(`Consumer:onMessage - Error: ${err.stack}`); }
                if (data) {
                    _self.Logger.log(`Consumer:onMessage - Data: ${JSON.stringify(data)}`);
                    message.value = message.value.toString();
                    message.key = message.key.toString();
                    return ((cb1) ? cb1(message) : message);
                }
            });
        });
    }
}
