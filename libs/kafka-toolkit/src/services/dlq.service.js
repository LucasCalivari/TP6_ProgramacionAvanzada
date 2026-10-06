"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var DlqService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.DlqService = void 0;
const common_1 = require("@nestjs/common");
const kafkajs_1 = require("kafkajs");
let DlqService = DlqService_1 = class DlqService {
    logger = new common_1.Logger(DlqService_1.name);
    producer;
    constructor() {
        const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
        const kafka = new kafkajs_1.Kafka({
            clientId: `${process.env.KAFKA_CLIENT_ID || 'service'}-dlq`,
            brokers,
        });
        this.producer = kafka.producer();
    }
    async onModuleInit() {
        try {
            await this.producer.connect();
        }
        catch (err) {
            this.logger.warn(`Could not connect DLQ producer immediately: ${err.message}`);
        }
    }
    async onModuleDestroy() {
        await this.producer.disconnect();
    }
    async sendToDlq(topic, key, message, errorReason, headers = {}) {
        const dlqTopic = `${topic}.dlq`;
        this.logger.error(`Sending failed event to DLQ topic [${dlqTopic}]: ${errorReason}`);
        try {
            await this.producer.send({
                topic: dlqTopic,
                messages: [
                    {
                        key,
                        value: typeof message === 'string' ? message : JSON.stringify(message),
                        headers: {
                            ...headers,
                            'x-dlq-reason': errorReason,
                            'x-dlq-timestamp': new Date().toISOString(),
                            'x-original-topic': topic,
                        },
                    },
                ],
            });
            this.logger.log(`Successfully moved message to ${dlqTopic}`);
        }
        catch (err) {
            this.logger.error(`Failed to publish message to DLQ [${dlqTopic}]: ${err.message}`);
        }
    }
    async executeWithRetry(topic, key, message, fn) {
        const delays = [1000, 2000, 4000];
        let lastError;
        for (let attempt = 0; attempt <= delays.length; attempt++) {
            try {
                return await fn();
            }
            catch (err) {
                lastError = err;
                this.logger.warn(`Execution attempt ${attempt + 1} failed: ${err.message}`);
                if (attempt < delays.length) {
                    const delay = delays[attempt];
                    this.logger.log(`Waiting ${delay}ms before retry...`);
                    await new Promise((resolve) => setTimeout(resolve, delay));
                }
            }
        }
        await this.sendToDlq(topic, key, message, lastError?.message || 'Unknown error');
        return null;
    }
};
exports.DlqService = DlqService;
exports.DlqService = DlqService = DlqService_1 = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [])
], DlqService);
//# sourceMappingURL=dlq.service.js.map