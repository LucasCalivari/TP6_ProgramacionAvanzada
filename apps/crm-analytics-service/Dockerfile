FROM node:20-alpine AS builder
WORKDIR /app

COPY package*.json ./
COPY libs/contracts/package*.json ./libs/contracts/
COPY libs/kafka-toolkit/package*.json ./libs/kafka-toolkit/
COPY apps/crm-analytics-service/package*.json ./apps/crm-analytics-service/

RUN npm install

COPY tsconfig.json ./
COPY libs/contracts ./libs/contracts
COPY libs/kafka-toolkit ./libs/kafka-toolkit
COPY apps/crm-analytics-service ./apps/crm-analytics-service

RUN npm run build --workspace=libs/contracts
RUN npm run build --workspace=libs/kafka-toolkit
RUN npm run build --workspace=apps/crm-analytics-service

FROM node:20-alpine AS runner
WORKDIR /app
COPY --from=builder /app ./
CMD ["npm", "run", "start:prod", "--workspace=apps/crm-analytics-service"]
