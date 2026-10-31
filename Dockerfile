FROM node:22-alpine
WORKDIR /app
COPY server/package.json server/package-lock.json ./server/
RUN cd server && npm ci --omit=dev
COPY server/server.js ./server/server.js
COPY web ./web
ENV WEB_ROOT=/app/web PORT=8787
EXPOSE 8787
CMD ["node", "server/server.js"]
