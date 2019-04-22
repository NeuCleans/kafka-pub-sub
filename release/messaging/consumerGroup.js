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
const uuid_1 = require("uuid");
const hlProducer_1 = require("./hlProducer");
const defaultOpts_1 = require("./defaultOpts");
class ServiceConsumerGroup {
    static getClient() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            return this.client;
        });
    }
    static init(defaultTopic = 'default', defaultTopicOpts, consumerGroupOpts, clientIdPrefix, logger) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.client)
                return;
            this.Logger = (logger) ? logger : {
                log: (data) => { console.log(data); },
                error: (error) => { console.error(error); }
            };
            defaultTopic = (defaultTopic) ? defaultTopic : 'default';
            clientIdPrefix = (clientIdPrefix) ? clientIdPrefix : "TEST";
            yield hlProducer_1.ServiceHLProducer.init(defaultTopic, defaultTopicOpts, consumerGroupOpts.kafkaHost, clientIdPrefix, logger);
            this.Logger.log('Init ConsumerGroup...');
            this._client = new kafka_node_1.KafkaClient({
                kafkaHost: consumerGroupOpts.kafkaHost || process.env.KAFKA_HOST,
                clientId: `${clientIdPrefix}_${uuid_1.v4()}`
            });
            consumerGroupOpts = (consumerGroupOpts) ? Object.assign({}, defaultOpts_1.defaultKafkaConsumerGroupOpts, consumerGroupOpts) : defaultOpts_1.defaultKafkaConsumerGroupOpts;
            this.client = new kafka_node_1.ConsumerGroup(consumerGroupOpts, [defaultTopic]);
            this.client.client = this._client;
            yield this._onReady();
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
                yield hlProducer_1.ServiceHLProducer.createTopic(topic);
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
            this.Logger.log('ConsumerGroup:listen - listening...');
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
                        _self.Logger.log(`ConsumerGroup:onMessage - Message: ${JSON.stringify(message, null, 2)}`);
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
            this.Logger.log('ConsumerGroup:onError - listening for errors...');
            return new Promise((resolve, reject) => {
                _self.client.on('error', (err) => __awaiter(this, void 0, void 0, function* () {
                    _self.Logger.error(`ConsumerGroup:onError - ERROR: ${err.stack}`);
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
                    _self.Logger.log(`ConsumerGroup:onReady - Ready...`);
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
                        _self.Logger.error(`ConsumerGroup:addTopic - ${err.stack}`);
                        reject(err);
                    }
                    else {
                        _self.Logger.log(`ConsumerGroup:addTopic - Topic: ${JSON.stringify(data)} added`);
                        resolve();
                    }
                });
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
                        _self.Logger.log(`ConsumerGroup:topicExists - Topic Does Not Exist`);
                        reject(err);
                    }
                    else {
                        _self.Logger.log(`ConsumerGroup:topicExists - Topic (${topic}) Already Exists`);
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
                        _self.Logger.log(`ConsumerGroup:refreshMetadata - Successful`);
                        resolve();
                    }
                    else {
                        _self.Logger.error(`ConsumerGroup:refreshMetadata - ${err.stack}`);
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
                        _self.Logger.log(`ConsumerGroup:commit - ${JSON.stringify(data)}`);
                        resolve();
                    }
                    else {
                        _self.Logger.error(`ConsumerGroup:commit - ${err.stack}`);
                        reject(err);
                    }
                });
            });
        });
    }
}
exports.ServiceConsumerGroup = ServiceConsumerGroup;
//# sourceMappingURL=consumerGroup.js.map