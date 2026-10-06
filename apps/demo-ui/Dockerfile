FROM node:20-alpine AS builder
WORKDIR /app

COPY package*.json ./
COPY libs/contracts/package*.json ./libs/contracts/
COPY apps/demo-ui/package*.json ./apps/demo-ui/

RUN npm install

COPY tsconfig.json ./
COPY libs/contracts ./libs/contracts
COPY apps/demo-ui ./apps/demo-ui

RUN npm run build --workspace=libs/contracts
RUN npm run build --workspace=apps/demo-ui

FROM nginx:alpine AS runner
COPY --from=builder /app/apps/demo-ui/dist /usr/share/nginx/html
COPY apps/demo-ui/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 5173
CMD ["nginx", "-g", "daemon off;"]
