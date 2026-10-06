FROM node:20-alpine AS builder
WORKDIR /app

COPY package*.json ./
COPY libs/contracts/package*.json ./libs/contracts/
COPY apps/loyalty-service/package*.json ./apps/loyalty-service/

RUN npm install

COPY tsconfig.json ./
COPY libs/contracts ./libs/contracts
COPY apps/loyalty-service ./apps/loyalty-service

RUN npm run build --workspace=libs/contracts
RUN npm run build --workspace=apps/loyalty-service

FROM node:20-alpine AS runner
WORKDIR /app
COPY --from=builder /app ./
CMD ["npm", "run", "start:prod", "--workspace=apps/loyalty-service"]
