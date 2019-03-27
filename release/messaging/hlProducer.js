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
    static init(defaultTopic, defaultTopicOpts) {
        return __awaiter(this, void 0, void 0, function* () {
            const _self = this;
            yield new Promise((resolve, reject) => {
                if (_self.isConnected) {
                    resolve();
                }
                _self.Logger.log('Init HLProducer...');
                _self._client = new kafka_node_1.KafkaClient({
                    kafkaHost: process.env.KAFKA_HOST || 'localhost:9092',
                    clientId: `${_self.clientIdPrefix}_${uuid_1.v4()}`
                });
                _self.client = new kafka_node_1.Producer(_self._client, {
                    requireAcks: 1,
                    ackTimeoutMs: 100,
                    partitionerType: 2
                });
                _self._client.once('ready', () => __awaiter(this, void 0, void 0, function* () {
                    _self.Logger.log('HLProducer:onReady - Ready....');
                    _self.isConnected = true;
                    if (defaultTopic)
                        yield _self.createTopic(defaultTopic, defaultTopicOpts);
                    resolve();
                }));
                _self.client.on('error', (err) => {
                    _self.Logger.error(`HLProducer:onError - ERROR: ${err.stack}`);
                });
            });
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
        this.Logger.log("jsonData: " + JSON.stringify(jsonData, null, 2));
        return Buffer.from(JSON.stringify(jsonData));
    }
    static buildAMessageObject(data, toTopic, fromTopic, action) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            ;
            const _self = this;
            return new Promise((resolve) => {
                const record = {
                    topic: toTopic,
                    messages: _self.prepareMsgBuffer(data, action),
                };
                if (fromTopic)
                    record['key'] = fromTopic;
                resolve(record);
            });
        });
    }
    static createTopic(topic, kafkaTopicConfig) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            kafkaTopicConfig = (kafkaTopicConfig) ?
                Object.assign({}, defaultOpts_1.defaultKafkaTopicConfig, kafkaTopicConfig) : defaultOpts_1.defaultKafkaTopicConfig;
            const topicToCreate = Object.assign({ topic }, kafkaTopicConfig);
            yield new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                const cb = (error, data) => {
                    if (error) {
                        _self.Logger.error("HLProducer:createTopic - " + error.stack);
                        reject(error);
                    }
                    if (data) {
                        _self.Logger.log(`HLProducer:createTopic - Topic created: ${JSON.stringify(data)}`);
                        resolve();
                    }
                };
                _self._client.createTopics([topicToCreate], cb);
            }));
        });
    }
    static refreshTopic(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const cb = () => { };
            this._client.refreshMetadata([topic], cb);
        });
    }
    static send(records) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            yield new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                const cb = (error, data) => {
                    if (error) {
                        _self.Logger.error("HLProducer:send - " + error.stack);
                        reject(error);
                    }
                    ;
                    if (data) {
                        _self.Logger.log(`HLProducer:send - data sent: ${JSON.stringify(data)}`);
                        resolve();
                    }
                };
                _self.client.send(records, cb);
            }));
        });
    }
    static close() {
        if (!this.isConnected)
            return;
        const _self = this;
        this.client.close(() => {
            _self.isConnected = false;
            _self.Logger.log('HLProducer:close - Closed');
        });
    }
}
ServiceHLProducer.Logger = {
    log: (data) => { console.log(data); },
    error: (error) => { console.error(error); }
};
ServiceHLProducer.clientIdPrefix = "SAMPLE";
ServiceHLProducer.isConnected = false;
exports.ServiceHLProducer = ServiceHLProducer;
;
//# sourceMappingURL=hlProducer.js.map