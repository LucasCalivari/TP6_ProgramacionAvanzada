export declare class DlqService {
    private readonly logger;
    private producer;
    constructor();
    onModuleInit(): Promise<void>;
    onModuleDestroy(): Promise<void>;
    sendToDlq(topic: string, key: string, message: any, errorReason: string, headers?: Record<string, string>): Promise<void>;
    executeWithRetry<T>(topic: string, key: string, message: any, fn: () => Promise<T>): Promise<T | null>;
}
