FROM node:22-alpine

WORKDIR /app

COPY benchmarks/portable/pgboss-bench/package.json .
COPY benchmarks/portable/pgboss-bench/package-lock.json .
RUN npm ci --omit=dev

COPY benchmarks/portable/pgboss-bench/main.js .

CMD ["node", "main.js"]
