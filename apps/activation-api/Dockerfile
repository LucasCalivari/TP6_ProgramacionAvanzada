FROM node:20-alpine AS builder
WORKDIR /app

COPY package*.json ./
COPY libs/contracts/package*.json ./libs/contracts/
COPY libs/kafka-toolkit/package*.json ./libs/kafka-toolkit/
COPY apps/activation-api/package*.json ./apps/activation-api/

RUN npm install

COPY tsconfig.json ./
COPY libs/contracts ./libs/contracts
COPY libs/kafka-toolkit ./libs/kafka-toolkit
COPY apps/activation-api ./apps/activation-api

RUN npm run build --workspace=libs/contracts
RUN npm run build --workspace=libs/kafka-toolkit
RUN npm run build --workspace=apps/activation-api

FROM node:20-alpine AS runner
WORKDIR /app
COPY --from=builder /app ./
EXPOSE 3000
CMD ["npm", "run", "start:prod", "--workspace=apps/activation-api"]
