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
    static init(defaultTopic, kHost, clientIdPrefix, logger, createProducer = true) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.client)
                return;
            this.Logger = (logger) ? logger : {
                log: (data) => { console.log(data); },
                error: (error) => { console.error(error); }
            };
            clientIdPrefix = (clientIdPrefix) ? clientIdPrefix : "TEST";
            if (createProducer) {
                yield producer_1.ServiceProducer.init(defaultTopic, kHost, clientIdPrefix, logger);
            }
            this.Logger.log('Init Consumer...');
            this._client = new kafka_node_1.KafkaClient({
                kafkaHost: kHost || process.env.KAFKA_HOST,
                clientId: `${clientIdPrefix}_${uuid_1.v4()}`,
            });
            this.client = new kafka_node_1.Consumer(this._client, [], {
                autoCommit: false,
                fromOffset: true
            });
            yield this._onReady();
            if (defaultTopic)
                yield this.subscribe(defaultTopic);
        });
    }
    static subscribe(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            try {
                yield this._topicExists(topic);
            }
            catch (error) {
                yield producer_1.ServiceProducer.createTopic(topic);
            }
            yield this._addTopic(topic);
        });
    }
    static commit(cb) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            yield this._commit();
            if (cb)
                cb();
        });
    }
    static listen(cb, commit = true) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            this.Logger.log('Consumer:listen - listening...');
            yield this._onMessage(cb, commit);
        });
    }
    static _onMessage(cb, commit) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            return new Promise((resolve, reject) => {
                _self.client.on('message', (message) => __awaiter(this, void 0, void 0, function* () {
                    if (message) {
                        if (message.hasOwnProperty('value') && message.value)
                            message.value = message.value.toString();
                        if (message.hasOwnProperty('key') && message.key)
                            message.key = message.key.toString();
                        _self.Logger.log(`Consumer:onMessage - Message: ${JSON.stringify(message, null, 2)}`);
                        if (commit)
                            yield this._commit();
                        resolve(cb(message));
                    }
                }));
            });
        });
    }
    static onError(cb) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            this.Logger.log('Consumer:onError - listening for errors...');
            return new Promise((resolve, reject) => {
                _self.client.on('error', (err) => __awaiter(this, void 0, void 0, function* () {
                    _self.Logger.error(`Consumer:onError - ERROR: ${err.stack}`);
                    (cb) ? resolve(cb(err)) : reject(err);
                }));
            });
        });
    }
    static _onReady() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            return new Promise((resolve, reject) => {
                _self._client.on('ready', () => __awaiter(this, void 0, void 0, function* () {
                    _self.Logger.log(`Consumer:onReady - Ready...`);
                    resolve();
                }));
            });
        });
    }
    static _addTopic(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            topic = Array.isArray(topic) ? topic : [topic];
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            return new Promise((resolve, reject) => {
                _self.client.addTopics(topic, (err, data) => {
                    if (err) {
                        _self.Logger.error(`Consumer:addTopic - ${err.stack}`);
                        reject(err);
                    }
                    else {
                        _self.Logger.log(`Consumer:addTopic - Topic: ${JSON.stringify(data)} added`);
                        resolve();
                    }
                }, false);
            });
        });
    }
    static _topicExists(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            return new Promise((resolve, reject) => {
                _self._client.topicExists([topic], (err) => {
                    if (err) {
                        _self.Logger.log(`Consumer:topicExists - Topic Does Not Exist`);
                        reject(err);
                    }
                    else {
                        _self.Logger.log(`Consumer:topicExists - Topic (${topic}) Already Exists`);
                        resolve();
                    }
                });
            });
        });
    }
    static _refreshMetadata(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            return new Promise((resolve, reject) => {
                _self._client.refreshMetadata([topic], (err) => {
                    if (!err) {
                        _self.Logger.log(`Consumer:refreshMetadata - Successful`);
                        resolve();
                    }
                    else {
                        _self.Logger.error(`Consumer:refreshMetadata - ${err.stack}`);
                        reject(err);
                    }
                });
            });
        });
    }
    static _commit() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            return new Promise((resolve, reject) => {
                _self.client.commit((err, data) => {
                    if (!err) {
                        _self.Logger.log(`Consumer:commit - ${JSON.stringify(data)}`);
                        resolve();
                    }
                    else {
                        _self.Logger.error(`Consumer:commit - ${err.stack}`);
                        reject(err);
                    }
                });
            });
        });
    }
}
exports.ServiceConsumer = ServiceConsumer;
//# sourceMappingURL=consumer.js.map