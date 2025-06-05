const fastify = require('fastify');
const redis = require('redis');
const path = require('path');
const axios = require('axios');
require('dotenv').config();

// Default configuration
let processingIntervalSeconds = 10;  // X seconds - how often to process all groups
let lockTimeoutMinutes = 5;          // Y minutes - how long a group stays locked
let groupProcessingDelaySeconds = 1; // Z seconds - delay between processing groups
let redisPort = 6381;                // Redis port
let serverPort = 3003;               // Server port

// Redis client
let redisClient;

// Initialize Redis connection and configuration
async function setupRedis() {
  try {
    const redisHost = process.env.REDIS_HOST || 'localhost';
    const redisPort = process.env.REDIS_PORT || 6381;
    redisClient = redis.createClient({ url: `redis://${redisHost}:${redisPort}` });

    console.log(`Connecting to Redis at ${redisHost}:${redisPort}`);

    redisClient.on('connect', () => {
      console.log('Redis connected successfully');
      server.log.info('Redis connected successfully');
    });

    redisClient.on('error', (err) => {
      console.log('Redis Client Error', err);
      server.log.error(`Redis Client Error: ${err.message}`);
    });

    // Wait for connection
    await redisClient.connect();
    server.log.info('Redis connected successfully');

    // Validate and fix Redis data types
    await validateAndFixRedisDataTypes();

    // Load configuration if it exists now
    try {
      const configHash = await safeRedisOperation(
        redisClient.hGetAll.bind(redisClient),
        'config',
        'hash',
        {
          processingIntervalSeconds: processingIntervalSeconds.toString(),
          lockTimeoutMinutes: lockTimeoutMinutes.toString(),
          groupProcessingDelaySeconds: groupProcessingDelaySeconds.toString()
        }
      );

      processingIntervalSeconds = parseInt(configHash.processingIntervalSeconds) || processingIntervalSeconds;
      lockTimeoutMinutes = parseInt(configHash.lockTimeoutMinutes) || lockTimeoutMinutes;
      groupProcessingDelaySeconds = parseInt(configHash.groupProcessingDelaySeconds) || groupProcessingDelaySeconds;
      server.log.info('Loaded configuration from Redis');
    } catch (err) {
      server.log.error(`Error loading config from Redis: ${err.message}`);
    }

    return true;
  } catch (err) {
    server.log.error(`Failed to connect to Redis: ${err.message}`);
    console.error('Redis setup failed:', err);
    return false;
  }
}

// Safe Redis operations with type checking
async function safeRedisOperation(operation, key, expectedType, fallbackValue, ...args) {
  try {
    const keyType = await redisClient.type(key);

    // If key doesn't exist, it's safe to proceed
    if (keyType === 'none') {
      return await operation(key, ...args);
    }

    // If key exists but is wrong type, delete and recreate
    if (keyType !== expectedType) {
      server.log.warn(`Key '${key}' has incorrect type '${keyType}', expected '${expectedType}', resetting it`);
      await redisClient.del(key);

      // For operations that might need re-initialization
      if (fallbackValue !== undefined) {
        if (expectedType === 'set' && operation === redisClient.sMembers.bind(redisClient)) {
          await redisClient.sAdd(key, fallbackValue);
        } else if (expectedType === 'hash' && operation === redisClient.hGetAll.bind(redisClient)) {
          for (const [hKey, hValue] of Object.entries(fallbackValue)) {
            await redisClient.hSet(key, hKey, hValue);
          }
        } else if (expectedType === 'list' && operation === redisClient.lRange.bind(redisClient)) {
          await redisClient.rPush(key, fallbackValue);
        }
      }

      return await operation(key, ...args);
    }

    // Type is correct, proceed
    return await operation(key, ...args);
  } catch (err) {
    server.log.error(`Error in safe Redis operation for key '${key}': ${err.message}`);
    if (fallbackValue !== undefined) {
      return fallbackValue;
    }
    throw err;
  }
}

// Setup the server
const server = fastify({
  logger: true
});

// Register static files plugin
server.register(require('@fastify/static'), {
  root: path.join(__dirname, 'public'),
  prefix: '/'
});

// Serve HTML on root
server.get('/', (request, reply) => {
  reply.sendFile('index.html');
});

// Queue endpoint
server.post('/queue', async (request, reply) => {
  try {
    const { globalThreadIdDestination, data } = request.body;

    if (!globalThreadIdDestination) {
      return reply.code(400).send({ error: 'globalThreadIdDestination is required' });
    }

    const queueItem = JSON.stringify({ globalThreadIdDestination, data });
    await safeRedisOperation(
      redisClient.rPush.bind(redisClient),
      `queue:${globalThreadIdDestination}`,
      'list',
      queueItem,
      queueItem
    );

    return { success: true, message: 'Added to queue' };
  } catch (error) {
    server.log.error(`Error in /queue: ${error.message}`);
    return reply.code(500).send({ error: 'Internal Server Error', message: error.message });
  }
});

// Unlock endpoint
server.post('/api/unlock/:globalThreadIdDestination', async (request, reply) => {
  try {
    const { globalThreadIdDestination } = request.params;

    await redisClient.del(`lock:${globalThreadIdDestination}`);

    return { success: true, message: `Unlocked group ${globalThreadIdDestination}` };
  } catch (error) {
    server.log.error(`Error in /api/unlock: ${error.message}`);
    return reply.code(500).send({ error: 'Internal Server Error', message: error.message });
  }
});

// API to get status
server.get('/api/status', async (request, reply) => {
  try {
    // Check if Redis is connected
    if (!redisClient.isReady) {
      return reply.code(500).send({
        error: 'Redis not connected',
        message: 'The server lost connection to Redis'
      });
    }

    const status = {};
    let lockedGroups = 0;

    // Get all queue keys
    const queueKeys = await redisClient.keys('queue:*');
    const queueGroups = queueKeys.map(key => key.replace('queue:', ''));

    // Get all lock keys to find locked groups with empty queues
    const lockKeys = await redisClient.keys('lock:*');
    const lockedGroupIds = lockKeys.map(key => key.replace('lock:', ''))
      .filter(id => !id.startsWith('processing:')); // Filter out processing locks

    // Get processing keys
    const processingKeys = await redisClient.keys('processing:*');
    const processingGroups = processingKeys
      .filter(key => key !== 'processing:all')
      .map(key => key.replace('processing:', ''));
    const isGlobalProcessing = processingKeys.includes('processing:all');

    // Combine unique group IDs from both queues and locks
    const allGroupIds = [...new Set([...queueGroups, ...lockedGroupIds])];

    // Get status for each group
    for (const group of allGroupIds) {
      const isLocked = await redisClient.exists(`lock:${group}`);
      const isProcessing = processingGroups.includes(group);
      const queueLength = await safeRedisOperation(
        redisClient.lLen.bind(redisClient),
        `queue:${group}`,
        'list',
        0
      );

      status[group] = {
        locked: isLocked === 1,
        processing: isProcessing,
        queueLength
      };

      if (isLocked === 1) {
        lockedGroups++;
      }
    }

    // Get webhooks with safe operation
    const webhooks = await safeRedisOperation(
      redisClient.sMembers.bind(redisClient),
      'webhooks',
      'set',
      ['http://localhost:3001/webhook']
    );

    // Get config with safe operation
    const configHash = await safeRedisOperation(
      redisClient.hGetAll.bind(redisClient),
      'config',
      'hash',
      {
        processingIntervalSeconds: processingIntervalSeconds.toString(),
        lockTimeoutMinutes: lockTimeoutMinutes.toString(),
        groupProcessingDelaySeconds: groupProcessingDelaySeconds.toString()
      }
    );

    const config = {
      processingIntervalSeconds: parseInt(configHash.processingIntervalSeconds || processingIntervalSeconds),
      lockTimeoutMinutes: parseInt(configHash.lockTimeoutMinutes || lockTimeoutMinutes),
      groupProcessingDelaySeconds: parseInt(configHash.groupProcessingDelaySeconds || groupProcessingDelaySeconds)
    };

    return {
      totalGroups: allGroupIds.length,
      lockedGroups,
      isGlobalProcessing,
      processingGroups,
      groups: status,
      webhooks,
      config
    };
  } catch (error) {
    server.log.error(`Error in /api/status: ${error.message}`);
    return reply.code(500).send({
      error: 'Internal Server Error',
      message: error.message
    });
  }
});

// Helper function to validate and fix Redis data types
async function validateAndFixRedisDataTypes() {
  try {
    // Check 'config' - should be a HASH
    const configType = await redisClient.type('config');
    if (configType !== 'hash' && configType !== 'none') {
      server.log.warn("'config' key has incorrect type, resetting it");
      await redisClient.del('config');
      await redisClient.hSet('config', 'processingIntervalSeconds', processingIntervalSeconds);
      await redisClient.hSet('config', 'lockTimeoutMinutes', lockTimeoutMinutes);
      await redisClient.hSet('config', 'groupProcessingDelaySeconds', groupProcessingDelaySeconds);
    } else if (configType === 'none') {
      // Create if not exists
      await redisClient.hSet('config', 'processingIntervalSeconds', processingIntervalSeconds);
      await redisClient.hSet('config', 'lockTimeoutMinutes', lockTimeoutMinutes);
      await redisClient.hSet('config', 'groupProcessingDelaySeconds', groupProcessingDelaySeconds);
    }

    // Check 'webhooks' - should be a SET
    const webhooksType = await redisClient.type('webhooks');
    if (webhooksType !== 'set' && webhooksType !== 'none') {
      server.log.warn("'webhooks' key has incorrect type, resetting it");
      await redisClient.del('webhooks');
      await redisClient.sAdd('webhooks', 'http://localhost:3001/webhook');
    } else if (webhooksType === 'none') {
      // Create if not exists
      await redisClient.sAdd('webhooks', 'http://localhost:3001/webhook');
    }

    // Check all queue keys - should be LISTs
    const queueKeys = await redisClient.keys('queue:*');
    for (const queueKey of queueKeys) {
      const keyType = await redisClient.type(queueKey);
      if (keyType !== 'list') {
        server.log.warn(`Queue key '${queueKey}' has incorrect type, resetting it`);
        await redisClient.del(queueKey);
        // We'll let it be recreated normally when new items are added
      }
    }

    return true;
  } catch (err) {
    server.log.error(`Error validating Redis data types: ${err.message}`);
    return false;
  }
}

// API to update webhooks
server.post('/api/webhooks', async (request, reply) => {
  try {
    const { action, webhook } = request.body;

    if (!webhook) {
      return reply.code(400).send({ error: 'webhook URL is required' });
    }

    if (action === 'add') {
      await safeRedisOperation(
        redisClient.sAdd.bind(redisClient),
        'webhooks',
        'set',
        webhook,
        webhook
      );
      return { success: true, message: 'Webhook added' };
    } else if (action === 'remove') {
      await safeRedisOperation(
        redisClient.sRem.bind(redisClient),
        'webhooks',
        'set',
        webhook,
        webhook
      );
      return { success: true, message: 'Webhook removed' };
    } else {
      return reply.code(400).send({ error: 'Invalid action. Use "add" or "remove"' });
    }
  } catch (error) {
    server.log.error(`Error in /api/webhooks: ${error.message}`);
    return reply.code(500).send({ error: 'Internal Server Error', message: error.message });
  }
});

// API to update config
server.post('/api/config', async (request, reply) => {
  try {
    const { processingIntervalSeconds: newX, lockTimeoutMinutes: newY, groupProcessingDelaySeconds: newZ } = request.body;

    if (newX) {
      processingIntervalSeconds = parseInt(newX);
      await safeRedisOperation(
        redisClient.hSet.bind(redisClient),
        'config',
        'hash',
        null,
        'processingIntervalSeconds',
        processingIntervalSeconds
      );
    }

    if (newY) {
      lockTimeoutMinutes = parseInt(newY);
      await safeRedisOperation(
        redisClient.hSet.bind(redisClient),
        'config',
        'hash',
        null,
        'lockTimeoutMinutes',
        lockTimeoutMinutes
      );
    }

    if (newZ) {
      groupProcessingDelaySeconds = parseInt(newZ);
      await safeRedisOperation(
        redisClient.hSet.bind(redisClient),
        'config',
        'hash',
        null,
        'groupProcessingDelaySeconds',
        groupProcessingDelaySeconds
      );
    }

    // Restart interval with new settings
    if (newX && processingInterval) {
      clearInterval(processingInterval);
      processingInterval = setInterval(processAllGroups, processingIntervalSeconds * 1000);
    }

    return { success: true, config: { processingIntervalSeconds, lockTimeoutMinutes, groupProcessingDelaySeconds } };
  } catch (error) {
    server.log.error(`Error in /api/config: ${error.message}`);
    return reply.code(500).send({ error: 'Internal Server Error', message: error.message });
  }
});

// Attempt to acquire a lock with a given key and timeout
async function acquireLock(lockKey, timeoutSeconds) {
  // Use SET NX (Not eXists) to ensure atomic lock acquisition
  const result = await redisClient.set(lockKey, 'locked', {
    NX: true,  // Only set if key doesn't exist
    EX: timeoutSeconds // Expiry in seconds
  });

  // If result is OK, we acquired the lock
  return result === 'OK';
}

// Release a lock
async function releaseLock(lockKey) {
  await redisClient.del(lockKey);
}

// Process a group - ONE ITEM AT A TIME
async function processGroup(globalThreadIdDestination) {
  // Try to acquire processing lock for this specific group
  const processingLockKey = `processing:${globalThreadIdDestination}`;
  const lockAcquired = await acquireLock(processingLockKey, 60); // 60 seconds timeout for processing

  if (!lockAcquired) {
    server.log.info(`Group ${globalThreadIdDestination} is already being processed by another instance, skipping`);
    return;
  }

  try {
    // Check if the group is locked
    const isLocked = await redisClient.exists(`lock:${globalThreadIdDestination}`);

    if (isLocked === 1) {
      server.log.info(`Group ${globalThreadIdDestination} is locked, skipping`);
      return;
    }

    // Check if the queue has any items
    const queueLength = await safeRedisOperation(
      redisClient.lLen.bind(redisClient),
      `queue:${globalThreadIdDestination}`,
      'list',
      0
    );

    if (queueLength === 0) {
      server.log.info(`No items in queue for group ${globalThreadIdDestination}, skipping`);
      return;
    }

    // Lock the group for Y minutes
    await redisClient.set(`lock:${globalThreadIdDestination}`, 'locked', {
      EX: lockTimeoutMinutes * 60 // Convert minutes to seconds
    });

    server.log.info(`Processing group ${globalThreadIdDestination}`);

    // Get ONLY THE FIRST item from the queue
    const itemStr = await safeRedisOperation(
      redisClient.lIndex.bind(redisClient),
      `queue:${globalThreadIdDestination}`,
      'list',
      null,
      0
    );

    if (!itemStr) {
      server.log.info(`No item found in queue for group ${globalThreadIdDestination}, unlocking`);
      await redisClient.del(`lock:${globalThreadIdDestination}`);
      return;
    }

    try {
      const item = JSON.parse(itemStr);

      // Get all webhooks
      const webhooks = await safeRedisOperation(
        redisClient.sMembers.bind(redisClient),
        'webhooks',
        'set',
        ['http://localhost:3001/webhook']
      );

      // Process the single item
      for (const webhook of webhooks) {
        try {
          await axios.post(webhook, item);
          server.log.info(`Successfully sent item to webhook ${webhook}`);
        } catch (error) {
          server.log.error(`Error sending to webhook ${webhook}: ${error.message}`);
        }
      }

      // Remove ONLY THE PROCESSED item from the queue
      await safeRedisOperation(
        redisClient.lPop.bind(redisClient),
        `queue:${globalThreadIdDestination}`,
        'list',
        null
      );

      server.log.info(`Processed 1 item from group ${globalThreadIdDestination}`);
      server.log.info(`${queueLength - 1} items remaining in queue for group ${globalThreadIdDestination}`);
    } catch (parseError) {
      server.log.error(`Error parsing item from queue for group ${globalThreadIdDestination}: ${parseError.message}`);

      // Remove the malformed item and continue
      await safeRedisOperation(
        redisClient.lPop.bind(redisClient),
        `queue:${globalThreadIdDestination}`,
        'list',
        null
      );
    }

    // The group remains locked until timeout or manual unlock
    // This ensures only one item is processed per unlock cycle
  } finally {
    // Always release the processing lock when done
    await releaseLock(processingLockKey);
  }
}

// Process all groups
async function processAllGroups() {
  // Try to acquire global processing lock
  const globalProcessingLockKey = 'processing:all';
  const lockAcquired = await acquireLock(globalProcessingLockKey, 300); // 5 minutes timeout

  if (!lockAcquired) {
    server.log.info('Another instance is already processing all groups, skipping this run');
    return;
  }

  try {
    server.log.info('Starting to process all groups');

    // Get all queue keys
    const queueKeys = await redisClient.keys('queue:*');
    const groups = queueKeys.map(key => key.replace('queue:', ''));

    server.log.info(`Found ${groups.length} groups to process`);

    // Process each group with delay between them
    for (const group of groups) {
      await processGroup(group);

      // Add delay between processing groups
      if (groupProcessingDelaySeconds > 0) {
        await new Promise(resolve => setTimeout(resolve, groupProcessingDelaySeconds * 1000));
      }
    }

    server.log.info('Finished processing all groups');
  } catch (error) {
    server.log.error(`Error in processAllGroups: ${error.message}`);
  } finally {
    // Always release the global processing lock when done
    await releaseLock(globalProcessingLockKey);
  }
}

// Start processing interval
let processingInterval;

// Graceful shutdown handler
function setupGracefulShutdown() {
  async function shutdown() {
    console.log('Shutting down gracefully...');

    // Clear the processing interval
    if (processingInterval) {
      clearInterval(processingInterval);
    }

    // Wait for any pending operations to complete
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Close Redis connection
    if (redisClient && redisClient.isReady) {
      await redisClient.quit();
      console.log('Redis connection closed');
    }

    // Close server
    await server.close();
    console.log('Server stopped');

    process.exit(0);
  }

  // Listen for termination signals
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

// Start the server
const start = async () => {
  try {
    const redisConnected = await setupRedis();

    if (!redisConnected) {
      server.log.error('Failed to connect to Redis. Check if Redis server is running.');
      console.error('Failed to connect to Redis. Check if Redis server is running.');
      process.exit(1);
    }

    // Setup graceful shutdown
    setupGracefulShutdown();

    // Start processing interval
    processingInterval = setInterval(processAllGroups, processingIntervalSeconds * 1000);
    server.log.info(`Started processing interval: ${processingIntervalSeconds} seconds`);

    // Start server
    await server.listen({ port: serverPort, host: '0.0.0.0' });
    console.log(`Server listening at http://localhost:${serverPort}`);
    console.log(`Redis connected at localhost:${redisPort}`);
    console.log(`Processing interval: ${processingIntervalSeconds} seconds`);
    console.log(`Lock timeout: ${lockTimeoutMinutes} minutes`);
    console.log(`Group processing delay: ${groupProcessingDelaySeconds} seconds`);

  } catch (err) {
    server.log.error(err);
    console.error('Server startup failed:', err);
    process.exit(1);
  }
};

start();