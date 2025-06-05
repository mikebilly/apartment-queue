# Use Node.js LTS Alpine base
FROM node:18-alpine

# Install curl for healthchecks
RUN apk add --no-cache curl

# Set working directory
WORKDIR /app

# Create logs directory
RUN mkdir -p /app/logs

# Copy package files
COPY package*.json ./
RUN npm install --include=dev

# Copy rest of the source code (can also rely on volume in compose)
COPY . .

# Start in dev mode
CMD ["npm", "run", "dev"]
