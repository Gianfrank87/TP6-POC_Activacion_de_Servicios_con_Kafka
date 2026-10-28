FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
COPY apps ./apps
COPY libs ./libs
COPY tsconfig.json ./
RUN npm install
CMD ["npm", "run", "start:activation"]
