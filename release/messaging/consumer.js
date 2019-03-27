"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const kafka_node_1 = require("kafka-node");
const producer_1 = require("./producer");
const uuid_1 = require("uuid");
class ServiceConsumer {
    static getClient() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            return this.client;
        });
    }
    static init(defaultTopic) {
        return __awaiter(this, void 0, void 0, function* () {
            const _self = this;
            yield new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                producer_1.ServiceProducer.Logger = _self.Logger;
                yield producer_1.ServiceProducer.init(defaultTopic)
                    .then(() => {
                    _self.Logger.log('Init Consumer...');
                    _self._client = new kafka_node_1.KafkaClient({
                        kafkaHost: process.env.KAFKA_HOST || 'localhost:9092',
                        clientId: `${_self.clientIdPrefix}_${uuid_1.v4()}`
                    });
                    _self.client = new kafka_node_1.Consumer(_self._client, [], {
                        autoCommit: false,
                        fromOffset: true
                    });
                    _self._client.once('ready', () => __awaiter(this, void 0, void 0, function* () {
                        _self.Logger.log(`Consumer:onReady - Ready...`);
                        if (defaultTopic)
                            yield _self.subscribe(defaultTopic);
                        resolve();
                    }));
                    _self.client.on('error', (err) => {
                        _self.Logger.error(`Consumer:onError - ERROR: ${err.stack}`);
                    });
                });
            }));
        });
    }
    static subscribe(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            yield new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                const cb = (err) => {
                    if (!err) {
                        resolve(_self.addTopic(topic));
                    }
                    else {
                        producer_1.ServiceProducer.createTopic(topic)
                            .then(() => {
                            resolve(_self.addTopic(topic));
                        });
                    }
                };
                this._client.topicExists([topic], cb);
            }));
        });
    }
    static addTopic(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            const cb = (err, data) => {
                if (err) {
                    _self.Logger.error(`Consumer:addTopic - ${err.stack}`);
                }
                if (data)
                    _self.Logger.log(`Consumer:addTopic - Topic: ${JSON.stringify(data)} added`);
            };
            this._client.refreshMetadata([topic], (err) => {
                if (!err) {
                    _self.client.addTopics([{ topic: topic, partition: 0, offset: 0 }], cb);
                }
            });
        });
    }
    static listen(cb1) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            this.client.on('message', (message) => {
                this.client.commit((err, data) => {
                    _self.Logger.log('Consumer:onMessage - Committing...');
                    if (err) {
                        _self.Logger.log(`Consumer:onMessage - Error: ${err.stack}`);
                    }
                    if (data) {
                        _self.Logger.log(`Consumer:onMessage - Data: ${JSON.stringify(data)}`);
                        message.value = message.value.toString();
                        message.key = message.key.toString();
                        return ((cb1) ? cb1(message) : message);
                    }
                });
            });
        });
    }
}
ServiceConsumer.Logger = {
    log: (data) => { console.log(data); },
    error: (error) => { console.error(error); }
};
ServiceConsumer.clientIdPrefix = "SAMPLE";
exports.ServiceConsumer = ServiceConsumer;
//# sourceMappingURL=consumer.js.map