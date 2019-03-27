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
class ServiceProducer {
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
            yield new Promise((resolve, reject) => {
                if (_self.isConnected) {
                    resolve();
                }
                _self.Logger.log('Init Producer...');
                _self._client = new kafka_node_1.KafkaClient({
                    kafkaHost: process.env.KAFKA_HOST || 'localhost:9092',
                    clientId: `${_self.clientIdPrefix}_${uuid_1.v4()}`
                });
                _self.client = new kafka_node_1.Producer(_self._client, {
                    requireAcks: 1,
                    ackTimeoutMs: 100
                });
                _self._client.once('ready', () => __awaiter(this, void 0, void 0, function* () {
                    _self.Logger.log('Producer:onReady - Ready....');
                    _self.isConnected = true;
                    if (defaultTopic)
                        yield _self.createTopic(defaultTopic);
                    resolve();
                }));
                _self.client.on('error', (err) => {
                    _self.Logger.error(`Producer:onError - ERROR: ${err.stack}`);
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
                    partition: 0,
                };
                if (fromTopic)
                    record['key'] = fromTopic;
                resolve(record);
            });
        });
    }
    static createTopic(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            yield new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                const cb = (error, data) => {
                    if (error) {
                        _self.Logger.error("Producer:createTopic - " + error.stack);
                        reject(error);
                    }
                    if (data) {
                        _self.Logger.log(`Producer:createTopic - Topic created: ${JSON.stringify(data)}`);
                        resolve();
                    }
                };
                _self.client.createTopics([topic], cb);
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
                        _self.Logger.error("Producer:send - " + error.stack);
                        reject(error);
                    }
                    ;
                    if (data) {
                        _self.Logger.log(`Producer:send - data sent: ${JSON.stringify(data)}`);
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
            _self.Logger.log('Producer:close - Closed');
        });
    }
}
ServiceProducer.Logger = {
    log: (data) => { console.log(data); },
    error: (error) => { console.error(error); }
};
ServiceProducer.clientIdPrefix = "SAMPLE";
ServiceProducer.isConnected = false;
exports.ServiceProducer = ServiceProducer;
;
//# sourceMappingURL=producer.js.map