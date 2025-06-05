# Dockerfile
FROM node:18-alpine

# Install curl (for healthchecks)
RUN apk add --no-cache curl

# Set working directory
WORKDIR /app

# Prepare logs directory
RUN mkdir -p /app/logs

# Copy and install dependencies
COPY package*.json ./
RUN npm install

# Copy application source
COPY . .

# Default command (for dev)
CMD ["npm", "run", "dev"]
