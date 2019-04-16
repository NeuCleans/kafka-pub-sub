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
    static init(defaultTopic, kHost, clientIdPrefix, logger) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.client)
                return;
            this.Logger = (logger) ? logger : {
                log: (data) => { console.log(data); },
                error: (error) => { console.error(error); }
            };
            clientIdPrefix = (clientIdPrefix) ? clientIdPrefix : "TEST";
            this.Logger.log('Init Producer...');
            this._client = new kafka_node_1.KafkaClient({
                kafkaHost: kHost || process.env.KAFKA_HOST,
                clientId: `${clientIdPrefix}_${uuid_1.v4()}`
            });
            this.client = new kafka_node_1.Producer(this._client, {
                requireAcks: 1,
                ackTimeoutMs: 100
            });
            yield this._onReady();
            this.isConnected = true;
            if (defaultTopic) {
                yield this.createTopic(defaultTopic);
            }
        });
    }
    static prepareMsgBuffer(data, action, opts) {
        let jsonData = {
            $ref: uuid_1.v4(),
            timestamp: Date.now(),
            data
        };
        if (action)
            jsonData['action'] = action;
        if (opts)
            jsonData['opts'] = opts;
        return Buffer.from(JSON.stringify(jsonData));
    }
    static buildAMessageObject(data, toTopic, fromTopic, action, opts) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            ;
            const record = {
                topic: toTopic,
                messages: this.prepareMsgBuffer(data, action, opts),
                partition: 0,
            };
            if (fromTopic)
                record['key'] = fromTopic;
            return record;
        });
    }
    static createTopic(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client) {
                yield this.init();
            }
            try {
                yield this._topicExists(topic);
            }
            catch (error) {
                yield this._createTopics(topic);
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
                    _self.Logger.error(`Producer:onError - ERROR: ${err.stack}`);
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
                    _self.Logger.log(`Producer:onReady - Ready...`);
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
                        _self.Logger.log(`Producer:topicExists - Topic Does Not Exist`);
                        reject(err);
                    }
                    else {
                        _self.Logger.log(`Producer:topicExists - Topic (${topic}) Already Exists`);
                        resolve();
                    }
                });
            });
        });
    }
    static _createTopics(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            topic = Array.isArray(topic) ? topic : [topic];
            if (!this.client) {
                yield this.init();
            }
            const _self = this;
            return new Promise((resolve, reject) => {
                _self.client.createTopics(topic, true, (err, data) => {
                    if (err) {
                        _self.Logger.error(`Producer:createTopics - ${err.stack}`);
                        reject(err);
                    }
                    else {
                        _self.Logger.log(`Producer:createTopics - Topics ${JSON.stringify(data)} created`);
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
                        _self.Logger.log(`Producer:refreshMetadata - Successful`);
                        resolve();
                    }
                    else {
                        _self.Logger.error(`Producer:refreshMetadata - ${err.stack}`);
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
                        _self.Logger.error("Producer:send - " + error.stack);
                        reject(error);
                    }
                    ;
                    if (data) {
                        _self.Logger.log(`Producer:send - data sent: ${JSON.stringify(data)}`);
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
            _self.Logger.log('Producer:close - Closed');
            if (cb) {
                cb();
            }
        });
    }
}
ServiceProducer.isConnected = false;
exports.ServiceProducer = ServiceProducer;
;
//# sourceMappingURL=producer.js.map