FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY common ./common
COPY services ./services
COPY simulator ./simulator
COPY scripts ./scripts
CMD ["node","services/worker.js"]
