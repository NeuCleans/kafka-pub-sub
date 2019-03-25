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
    static init() {
        return __awaiter(this, void 0, void 0, function* () {
            const _self = this;
            yield new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                hlProducer_1.ServiceHLProducer.Logger = _self.Logger;
                hlProducer_1.ServiceHLProducer.SERVICE_ID = _self.SERVICE_ID;
                yield hlProducer_1.ServiceHLProducer.init()
                    .then(() => {
                    _self.Logger.log('Init ConsumerGroup...');
                    _self._client = new kafka_node_1.KafkaClient({
                        kafkaHost: process.env.KAFKA_HOST,
                        clientId: _self.SERVICE_ID
                    });
                    const options = Object.assign({}, _self.kafkaConsumerGroupOpts, { kafkaHost: process.env.KAFKA_HOST });
                    _self.client = new kafka_node_1.ConsumerGroup(options, [_self.SERVICE_ID]);
                    _self.client.client = _self._client;
                    _self._client.on('ready', () => {
                        _self.Logger.log(`ConsumerGroup:onReady - Ready...`);
                        resolve();
                    });
                    _self.client.on('error', (err) => {
                        _self.Logger.error(`ConsumerGroup:onError - ERROR: ${err.stack}`);
                    });
                });
            }));
        });
    }
    static subscribe(topic = this.SERVICE_ID) {
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
                        hlProducer_1.ServiceHLProducer.createTopic(topic)
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
                    _self.Logger.error(`ConsumerGroup:addTopic - ${err.stack}`);
                }
                if (data)
                    _self.Logger.log(`ConsumerGroup:addTopic - Topic: ${JSON.stringify(data)} added`);
            };
            this._client.refreshMetadata([topic], (err) => {
                if (!err) {
                    _self.client.addTopics([topic], cb);
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
                    _self.Logger.log('ConsumerGroup:onMessage - Committing...');
                    if (err) {
                        _self.Logger.log(`ConsumerGroup:onMessage - Error: ${err.stack}`);
                    }
                    if (data) {
                        _self.Logger.log(`ConsumerGroup:onMessage - Data: ${JSON.stringify(data)}`);
                        message.value = message.value.toString();
                        message.key = message.key.toString();
                        return ((cb1) ? cb1(message) : message);
                    }
                });
            });
        });
    }
}
ServiceConsumerGroup.Logger = {
    log: (data) => { console.log(data); },
    error: (error) => { console.error(error); }
};
ServiceConsumerGroup.SERVICE_ID = uuid_1.v4();
ServiceConsumerGroup.kafkaConsumerGroupOpts = defaultOpts_1.defaultKafkaConsumerGroupOpts;
exports.ServiceConsumerGroup = ServiceConsumerGroup;
//# sourceMappingURL=consumerGroup.js.map