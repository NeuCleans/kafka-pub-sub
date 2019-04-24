import { Logger } from './interfaces';
export declare class ServiceConsumerObject {
    private Logger;
    private client;
    private _client;
    constructor(kHost?: string, clientId?: string, logger?: Logger);
    subscribe(topic: string): Promise<void>;
    commit(cb?: Function): Promise<void>;
    listen(cb: (message: any) => any, commit?: boolean): Promise<void>;
    pauseTopic(topic: string): Promise<void>;
    resumeTopic(topic: string): Promise<void>;
    close(): Promise<void>;
    private _onMessage;
    onError(cb?: Function): Promise<{}>;
    private _onReady;
    private _addTopic;
    private _topicExists;
    private _refreshMetadata;
    private _commit;
    private _pauseTopic;
    private _resumeTopic;
    private _close;
}
