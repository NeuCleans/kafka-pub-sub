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
const defaultOpts_1 = require("./defaultOpts");
class ServiceHLProducer {
    static getClient() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            return this.client;
        });
    }
    static init(defaultTopic, defaultTopicOpts, kHost, clientIdPrefix, logger) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.client)
                return;
            this.Logger = (logger) ? logger : {
                log: (data) => { console.log(data); },
                error: (error) => { console.error(error); }
            };
            clientIdPrefix = (clientIdPrefix) ? clientIdPrefix : "TEST";
            this.Logger.log('Init HLProducer...');
            this._client = new kafka_node_1.KafkaClient({
                kafkaHost: kHost || process.env.KAFKA_HOST,
                clientId: `${clientIdPrefix}_${uuid_1.v4()}`
            });
            this.client = new kafka_node_1.Producer(this._client, {
                requireAcks: 1,
                ackTimeoutMs: 100,
                partitionerType: 2
            });
            yield this._onReady();
            this.isConnected = true;
            if (defaultTopic) {
                yield this.createTopic(defaultTopic, defaultTopicOpts);
            }
        });
    }
    static prepareMsgBuffer(data, action) {
        let jsonData = {
            $ref: uuid_1.v4(),
            timestamp: Date.now(),
            data: data
        };
        if (action)
            jsonData['action'] = action;
        return Buffer.from(JSON.stringify(jsonData));
    }
    static buildAMessageObject(data, toTopic, fromTopic, action) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            ;
            const _self = this;
            const record = {
                topic: toTopic,
                messages: _self.prepareMsgBuffer(data, action),
            };
            if (fromTopic)
                record['key'] = fromTopic;
            return record;
        });
    }
    static createTopic(topic, kafkaTopicConfig) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            try {
                yield this._topicExists(topic);
            }
            catch (error) {
                kafkaTopicConfig = (kafkaTopicConfig) ?
                    Object.assign({}, defaultOpts_1.defaultKafkaTopicConfig, kafkaTopicConfig) : defaultOpts_1.defaultKafkaTopicConfig;
                const topicToCreate = Object.assign({ topic }, kafkaTopicConfig);
                yield this._createTopics([topicToCreate]);
            }
        });
    }
    static send(records) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._send(records);
        });
    }
    static onError(cb) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            return new Promise((resolve, reject) => {
                _self.client.on('error', (err) => __awaiter(this, void 0, void 0, function* () {
                    _self.Logger.error(`HLProducer:onError - ERROR: ${err.stack}`);
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
                    _self.Logger.log(`HLProducer:onReady - Ready...`);
                    resolve();
                }));
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
                        _self.Logger.log(`HLProducer:topicExists - Topic Does Not Exist`);
                        reject(err);
                    }
                    else {
                        _self.Logger.log(`HLProducer:topicExists - Topic (${topic}) Already Exists`);
                        resolve();
                    }
                });
            });
        });
    }
    static _createTopics(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            return new Promise((resolve, reject) => {
                _self._client.createTopics(topic, (error, data) => __awaiter(this, void 0, void 0, function* () {
                    if (error) {
                        _self.Logger.error("HLProducer:createTopic - " + error.stack);
                        reject(error);
                    }
                    if (data) {
                        _self.Logger.log(`HLProducer:createTopic - Topic created: ${JSON.stringify(data)}`);
                        resolve();
                    }
                }));
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
                        _self.Logger.log(`HLProducer:refreshMetadata - Successful`);
                        resolve();
                    }
                    else {
                        _self.Logger.error(`HLProducer:refreshMetadata - ${err.stack}`);
                        reject(err);
                    }
                });
            });
        });
    }
    static _send(data) {
        return __awaiter(this, void 0, void 0, function* () {
            data = Array.isArray(data) ? data : [data];
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            return new Promise((resolve, reject) => {
                _self.client.send(data, (error, data) => {
                    if (error) {
                        _self.Logger.error("HLProducer:send - " + error.stack);
                        reject(error);
                    }
                    ;
                    if (data) {
                        _self.Logger.log(`HLProducer:send - data sent: ${JSON.stringify(data)}`);
                        resolve();
                    }
                });
            });
        });
    }
    static close(cb) {
        if (!this.isConnected)
            return;
        const _self = this;
        this.client.close(() => {
            _self.isConnected = false;
            _self.Logger.log('HLProducer:close - Closed');
            if (cb) {
                cb();
            }
        });
    }
}
ServiceHLProducer.isConnected = false;
exports.ServiceHLProducer = ServiceHLProducer;
;
//# sourceMappingURL=hlProducer.js.map