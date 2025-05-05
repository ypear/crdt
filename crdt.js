const level = require('level');
const fs = require('fs');
const path = require('path');

class CRDTPersistence {
  constructor(storagePath, { Y }) {
    if (!Y) throw new Error('Y.js instance (Y) is required');
    this.Y = Y;
    // Normalize and ensure storage directory exists
    this.storagePath = path.resolve(storagePath);
    try {
      fs.mkdirSync(this.storagePath, { recursive: true });
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
    }
    // Initialize LevelDB with error handling
    try {
      this.db = level(path.join(this.storagePath), {
        valueEncoding: 'binary'
      });
    }
    catch (err) {
      console.error('Failed to initialize LevelDB:', err);
      throw err;
    }
  }

  async storeUpdate(docName, update) {
    if (!(update instanceof Uint8Array)) {
      throw new Error('Update must be Uint8Array');
    }
    // Validate update structure
    const testDoc = new this.Y.Doc();
    try {
      this.Y.applyUpdate(testDoc, update);
    }
    catch (e) {
      console.error('Invalid CRDT update:', e);
      throw new Error(`Invalid CRDT update: ${e.message}`);
    }
    const timestamp = Date.now();
    const key = `doc_${docName}_update_${timestamp}`;
    try {
      // Get current state vector
      let currentSv;
      try {
        currentSv = await this.db.get(`doc_${docName}_sv`, { valueEncoding: 'binary' });
      }
      catch (err) {
        if (!err.notFound) throw err;
        currentSv = new Uint8Array(0);
      }
      // Calculate new state vector
      const tempDoc = new this.Y.Doc();
      if (currentSv.length > 0) {
        this.Y.applyUpdate(tempDoc, this.Y.encodeStateAsUpdate(new this.Y.Doc(), currentSv));
      }
      this.Y.applyUpdate(tempDoc, update);
      const newSv = this.Y.encodeStateVector(tempDoc);
      await this.db.batch([ // Store with batch operation
        { type: 'put', key, value: Buffer.from(update) },
        { type: 'put', key: `doc_${docName}_sv`, value: Buffer.from(newSv) },
        { 
          type: 'put', 
          key: `doc_${docName}_meta`, 
          value: Buffer.from(JSON.stringify({
            lastUpdated: timestamp,
            size: update.length
          }))
        }
      ]);
    }
    catch (err) {
      console.error('Error storing update:', err);
      throw err;
    }
  }

  async getYDoc(docName) {
    const doc = new this.Y.Doc();
    try {
      const updates = await this._getAllUpdates(docName);
      updates.forEach(update => {
        try {
          this.Y.applyUpdate(doc, update);
        }
        catch (e) {
          console.error('Error applying update:', e);
          throw e;
        }
      });
      return doc;
    }
    catch (err) {
      console.error('Error loading document:', err);
      throw err;
    }
  }

  async getStateVector(docName) {
    try {
      const value = await this.db.get(`doc_${docName}_sv`, { valueEncoding: 'binary' });
      return new Uint8Array(value);
    }
    catch (err) {
      if (err.notFound) return new Uint8Array(0);
      throw err;
    }
  }

  async _getAllUpdates(docName) {
    return new Promise((resolve, reject) => {
      const updates = [];
      this.db.createReadStream({
        gt: `doc_${docName}_update_`,
        lt: `doc_${docName}_update_\xff`,
        valueEncoding: 'binary'
      })
      .on('data', ({ value }) => {
        try {
          updates.push(new Uint8Array(value));
        }
        catch (e) {
          reject(e);
        }
      })
      .on('end', () => resolve(updates))
      .on('error', reject);
    });
  }

  async close() {
    try {
      await this.db.close();
    }
    catch (err) {
      console.error('Error closing persistence:', err);
      throw err;
    }
  }
}










/**
 * ypearCRDT - A module for creating a Conflict-Free Replicated Data Type (CRDT) over a Hyperswarm network.
 * 
 * This module uses Yjs for CRDT operations and integrates with a Hyperswarm network for decentralized synchronization.
 * It supports optional LevelDB persistence for data durability and provides methods to get, set, and delete data in a distributed map-like structure.
 * @param {Object} router - An instance of the `ypearRouter` module for network communication.
 * , {
  * @param {Object} options - Configuration options for the CRDT instance.
  * @param {string} options.topic - The topic to join in the Hyperswarm network for synchronization.
  * @param {string} [options.leveldb] - Path to a LevelDB database for persistent storage (optional).
  * @param {Function} [options.observerFunction] - A callback function to observe changes in the CRDT state.
  * @returns {Promise<Object>} - Resolves to a proxy object providing controlled access to the CRDT API.
 * }
 */
const ypearCRDT = async (router, options) => {
  return new Promise(async (resolve) => {

    if ([true].includes(options.leveldb)) options.leveldb = `./${options.topic}`; // true or custom /path/name or left out to use ram

    // Validate input options
    if (!router?.isYpearRouter) throw new Error('router must be an instance of ypearRouter module');

    // Import Yjs for CRDT operations
    let Y; 
    if(router.options.Y) Y = router.options.Y;
    else {
      router.updateOptions({ Y: require('yjs') });
      Y = router.options.Y;
    }
    
    // Declare variables for persistence, Yjs documents, handlers, and cache
    let 
      p      // persistence
    , y = {} // The Yjs document and its index (ix) in JSON format
    , h = {} // Handlers for Yjs maps/arrays and the index handler
    , c = {} // Cache for the current state of the CRDT
    , propagate, broadcast, forPeers, toPeer // Function to broadcast updates to the network
    ;
    const publicKey = router.options.publicKey;

    // Initialize LevelDB persistence if enabled
    if (options.leveldb) {
      p = new CRDTPersistence(options.leveldb, { Y });
      y = {
        doc: await p.getYDoc(options.leveldb), // Load or create the Yjs document
        ix: undefined // Initialize the index map later
      };
      const stateVector = await p.getStateVector(options.leveldb);
      if (stateVector && stateVector.length > 1) {  // Existing data in LevelDB
        h.ix = y.doc.getMap('ix'); // Get the index map
        y.ix = h.ix.toJSON(); // Convert the index map to JSON
      }
      else { // New data (no existing state)
        h.ix = y.doc.getMap('ix'); // Create a new index map
        y.ix = {}; // Initialize the index as an empty object
      }
      // Load existing maps and arrays from the index into handlers and cache
      c = {};
      if (Object.keys(y.ix).length > 0) {
        for (const [key, val] of Object.entries(y.ix)) { // Iterate over the index keys
          if (val == 'map') h[key] = y.doc.getMap(key); // Create a handler for each map
          else h[key] = y.doc.getArray(key); // or array
          c[key] = h[key].toJSON(); // Cache the current state of the map or array
        }
      }
    }
    else { // No LevelDB persistence
      y = {
        ix: undefined, // Index map handler
        doc: new Y.Doc() // Create a new Yjs document
      };
      h.ix = y.doc.getMap('ix'); // Initialize the index map
      y.ix = {}; // Initialize the index as an empty object
      c = {}; // Initialize the cache as an empty object
    }

    if (router.options.cache?.[options.topic]) { // has a database ontop
      options.topic += '-db';
    }
    else if (!router.started) await router.start(router.options.networkName); // happens if the the router and crdt are naked

    // inject this crdt as part of the router
    router.updateOptionsCache({
      [options.topic]: {
        synced: (options.topic.endsWith('-db') && !Object.keys(router.peers).length), // otherwise false
        sync: async (forPeers, topic) => { // Add this function to the createSwarm function
          console.log(router.options.username, 'syncing ...');
          await forPeers({ meta: 'ready', publicKey, stateVector: Y.encodeStateVector(y.doc) });
          return new Promise((resolve) => {
            if (router.options.cache[topic].synced) { // If already synced, resolve immediately
              resolve();
              return;
            }
            const checkSync = () => { // Otherwise, create a function to check sync status periodically
              if (router.options.cache[topic].synced) {
                console.log(router.options.username, 'is synced!');
                resolve();
              } else {
                setTimeout(checkSync, 50); // Check again after a short delay
              }
            };
            checkSync(); // Start checking
          });
        },
        peerStateVectors: {},
        updateStateVector: (publicKey, topic) => {
          const vector = Y.encodeStateVector(y.doc);
          router.options.cache[topic].peerStateVectors[publicKey] = vector;
          return Y.encodeStateAsUpdate(y.doc, vector);
        },
        setPeerStateVector(publicKey, vector, topic) { 
          if (!router.options.cache?.[topic].peerStateVectors) router.options.cache[topic].peerStateVectors = {};
          router.options.cache[topic].peerStateVectors[publicKey] = vector; 
        },
        peerClose: async function(publicKey) {
          if (router.options.cache[options.topic].peerStateVectors[publicKey]) {
            delete router.options.cache[options.topic].peerStateVectors[publicKey];
          }
        },
        // selfClose happens in the router ##selfClose
        selfClose: async function(publicKey) {
          if (options.leveldb) await p.close();
          await propagate({ publicKey, meta: 'cleanup' });
        }
      }
    });

    async function onData(d) {
      if (d.message) {
        if (options.observerFunction) {
          options.observerFunction(d); // Pass a message
        }
      }
      else if (d.meta == 'cleanup') await router.options.cache[options.topic].peerClose(d.publicKey);
      else if (d.meta == 'ready' && router.options.cache[options.topic].synced) {
        console.log(`${router.options.username} is the syncer`);
        const update = Y.encodeStateAsUpdate(y.doc, d.stateVector);
        router.options.cache[options.topic].setPeerStateVector(d.publicKey, Y.encodeStateVector(y.doc), options.topic);
        await toPeer(d.publicKey, { update, meta: 'sync' });
      }
      else if (d.update) {
        console.log(`${router.options.username} got updated`, d.meta);
        Y.applyUpdate(y.doc, d.update); // also handles cleanup on users end
        if (options.leveldb) await p.storeUpdate(options.leveldb, d.update); // Persist the update
        // Update local handlers and cache based on the index map
        const diffs = y.ix;
        for (const [key, value] of Object.entries(diffs)) { // Iterate over the index map
          if (!y.ix[key]) { // If the map/array is not in the local index
            y.ix[key] = value; // Add it to the index
            if (value == 'map') h[key] = y.doc.getMap(key); // Create a handler for the map
            else h[key] = y.doc.getArray(key); // Create a handler for the array
          }
          c[key] = h[key].toJSON(); // Update the cache
        }
        if (d.meta == 'sync' && !router.options.cache[options.topic].synced) router.options.cache[options.topic].synced = true;
        // Trigger the observer function if provided
        if (options.observerFunction) {
          options.observerFunction(Object.freeze({ ...c })); // Pass a frozen copy of the cache
        }
      }
    }
    
    // Join the Hyperswarm network and set up the broadcast handler
    [propagate, broadcast, forPeers, toPeer] = await router.alow(options.topic, async function(d) {
      await onData(d);
    });

    // Error message for protected CRDT names
    const errProtected = `crdt names 'ix, doc' are protected`;
    const errMapExpected = `this function is for 'map' only`;
    const errArrayExpected = `this function is for 'array' only`;
    const errArrayMethodExpected = `if you intend to use this function to manipulate an 'array' inside a 'map' you have malformed parameters`;

    // Create a transaction queue
    const batched = [];

    // Function to execute all queued transactions in a single transaction
    async function execBatch(throughDatabase) {
      return new Promise(async (resolve) => {
        if (batched.length === 0) return;
        await new Promise((exited) => {
          y.doc.transact(async () => {
            (async function ___ops(i) {
              if (batched[i]) {
                await batched[i]();
                setTimeout(() => { ___ops(i + 1); }, 0);
              }
              else {
                ___ops = null; // end
                exited();
              }
            })(0);
          });
        });
        batched.length = 0; // Clear the queue after execution
        const update = Y.encodeStateAsUpdate(y.doc);
        if (options.leveldb) await p.storeUpdate(options.leveldb, update); // Persist the update
        if (!throughDatabase) {
          await propagate({ update, meta: "batch" });
          resolve();
        }
        else resolve({ update, meta: "batch" });
      });
    }

    /**
     * Get or create a new map in the CRDT.
     *
     * @param {string} name - The name of the map to get or create.
     * @returns {Promise<void>} - Resolves when the map is ready.
     */
    async function map(name, batch) {
      return new Promise(async (done) => {
        if (['ix', 'doc'].includes(name)) throw new Error(errProtected); // Prevent overwriting protected names
        let changed = false;
        const operation = async () => {
          if (!h[name]) { // If the map doesn't exist
            h[name] = y.doc.getMap(name); // Create a new map
            changed = true;
          }
          c[name] = h[name].toJSON(); // Update the cache with the map's current state
          if (!y.ix[name]) { // If the map is not in the index
            y.ix[name] = 'map'; // Add it to the index
            h.ix.set(name, y.ix[name]); // Update the index map
            changed = true;
          }
        };
        if (batch) batched.push(operation);
        else {
          await operation();
          if (changed) {
            const update = Y.encodeStateAsUpdate(y.doc); // Encode the update
            if (options.leveldb) await p.storeUpdate(options.leveldb, update); // Persist the update
            await propagate({ update }); // Broadcast the update to the network
          }
        }
        done();
      });
    }
    
    /**
     * Set a key-value pair in a map.
     * 
     * @param {string} name - The name of the map.
     * @param {string} key - The key to set.
     * @param {any} val - The value to set.
     * @returns {Promise<void>} - Resolves when the update is complete.
     */
    async function set(name, key, val, batch, arrayMethod, optionalArrayParam0, optionalArrayParam1) {
      return new Promise(async (done) => {
        if (['insert', 'push', 'unshift', 'cut'].includes(batch)) {
          optionalArrayParam1 = optionalArrayParam0 + '';
          optionalArrayParam0 = arrayMethod + '';
          arrayMethod = batch + '';
        }
        if (['ix', 'doc'].includes(name)) throw new Error(errProtected); // Prevent overwriting protected names
        if (![undefined, 'map'].includes(y.ix[name])) throw new Error(errMapExpected); // limit this function to use with maps
        if (arrayMethod && Array.isArray(val)) { // insert an array as key
          if ((arrayMethod == 'insert' && typeof optionalArrayParam0 != 'number') 
          ||  (['push', 'unshift'].contains(arrayMethod) && (optionalArrayParam0 || optionalArrayParam1)) 
          ||  (arrayMethod == 'cut' && (typeof optionalArrayParam0 != 'number' || typeof optionalArrayParam1 != 'number' || val))) {
            throw new Error(errArrayMethodExpected);
          }
        }
        let changed = false;
        const operation = async () => {
          if (!h[name]) {
            await map(name); // create the map if it doesn't exist
            changed = true;
          }
          if (arrayMethod && Array.isArray(val)) { // insert an array as key
            if (!h[name].has(key)) h[name].set(key, new Y.Array());
            const yarray = h[name].get(key); // get the Yarray
            if (!Array.isArray(val)) val = [val]; // insert, push and unshift all expect val to be an array (so if you need to nest do: [[val]])
            if (arrayMethod == 'insert') yarray.insert(optionalArrayParam0, val);
            else if (arrayMethod == 'push') yarray.push(val);
            else if (arrayMethod == 'unshift') yarray.unshift(val);
            else if (arrayMethod == 'cut') yarray.delete(optionalArrayParam0, optionalArrayParam1);
            c[name][key] = yarray.toArray(); // Update the cache
            changed = true;
          }
          else {
            h[name].set(key, val); // Set the key-value pair in the map
            c[name][key] = val; // Update the cache
            changed = true;
          }
        };
        if (batch) batched.push(operation);
        else {
          await operation();
          if (changed) {
            const update = Y.encodeStateAsUpdate(y.doc); // Encode the update
            if (options.leveldb) await p.storeUpdate(options.leveldb, update); // Persist the update
            await propagate({ update }); // Broadcast the update to the network
          }
        }
        done();
      });
    }

    /**
     * Delete a key from a map.
     * 
     * @param {string} name - The name of the map.
     * @param {string} key - The key to delete.
     * @returns {Promise<void>} - Resolves when the update is complete.
     */
    async function del(name, key, batch) {
      return new Promise(async (done) => {
        if (['ix', 'doc'].includes(name)) throw new Error(errProtected); // Prevent overwriting protected names
        if (![undefined, 'map'].includes(y.ix[name])) throw new Error(errMapExpected); // limit this function to use with maps
        const operation = async () => {
          if (!h[name]) await map(name); // create the map if it doesn't exist
          h[name].delete(key); // Delete the key from the map
          delete c[name][key]; // Update the cache
        };
        if (batch) batched.push(operation);
        else {
          await operation();
          const update = Y.encodeStateAsUpdate(y.doc); // Encode the update
          if (options.leveldb) await p.storeUpdate(options.leveldb, update); // Persist the update
          await propagate({ update }); // Broadcast the update to the network
        }
        done();
      });
    }

    /**
     * Get or create a new array in the CRDT.
     *
     * @param {string} name - The name of the array to get or create.
     * @returns {Promise<void>} - Resolves when the array is ready.
     */
    async function array(name, batch) {
      return new Promise(async (done) => {
        if (['ix', 'doc'].includes(name)) throw new Error(errProtected); // Prevent overwriting protected names
        let changed = false;
        const operation = async () => {
          if (!h[name]) { // If the map doesn't exist
            h[name] = y.doc.getArray(name); // Create a new array
            changed = true;
          }
          c[name] = h[name].toJSON(); // Update the cache with the map's current state
          if (!y.ix[name]) { // If the map is not in the index
            y.ix[name] = 'array'; // Add it to the index
            h.ix.set(name, y.ix[name]); // Update the index map
            changed = true;
          }
        };
        if (batch) batched.push(operation);
        else {
          await operation();
          if (changed) {
            const update = Y.encodeStateAsUpdate(y.doc); // Encode the update
            if (options.leveldb) await p.storeUpdate(options.leveldb, update); // Persist the update
            await propagate({ update }); // Broadcast the update to the network
          }
        }
        done();
      });
    }

    /**
     * Inserts content at specified index in the named array
     * @param {string} name - Array identifier
     * @param {number} index - Position to insert at
     * @param {*} content - Content to insert
     * @returns {Promise} Resolves when operation completes
     */
    async function insert(name, val, index, batch) { // val is content meaning an array to insert at the index
      return new Promise(async (done) => {
        if (['ix', 'doc'].includes(name)) throw new Error(errProtected); // Prevent overwriting protected names
        if (![undefined, 'array'].includes(y.ix[name])) throw new Error(errArrayExpected); // limit this function to use with arrays
        const operation = async () => {
          if (!h[name]) await array(name); // create the array if it doesn't exist
          h[name].insert(index, val); // modify
          c[name] = h[name].toJSON(); // Update the cache
        };
        if (batch) batched.push(operation);
        else {
          await operation();
          const update = Y.encodeStateAsUpdate(y.doc); // Encode the update
          if (options.leveldb) await p.storeUpdate(options.leveldb, update); // Persist the update
          await propagate({ update }); // Broadcast the update to the network
        }
        done();
      });
    }

    /**
     * Adds value to end of the named array
     * @param {string} name - Array identifier  
     * @param {*} val - Value to append
     * @returns {Promise} Resolves when operation completes
     */
    async function push(name, val, batch) {
      return new Promise(async (done) => {
        if (['ix', 'doc'].includes(name)) throw new Error(errProtected); // Prevent overwriting protected names
        if (![undefined, 'array'].includes(y.ix[name])) throw new Error(errArrayExpected); // limit this function to use with arrays
        const operation = async () => {
          if (!h[name]) await array(name); // create the array if it doesn't exist
          if (!Array.isArray(val)) val = [val];
          h[name].push(val); // modify
          c[name] = h[name].toJSON(); // Update the cache
        };
        if (batch) batched.push(operation);
        else {
          await operation();
          const update = Y.encodeStateAsUpdate(y.doc); // Encode the update
          if (options.leveldb) await p.storeUpdate(options.leveldb, update); // Persist the update
          await propagate({ update }); // Broadcast the update to the network
        }
        done();
      });
    }

    /**
     * Adds value to beginning of the named array
     * @param {string} name - Array identifier
     * @param {*} val - Value to prepend
     * @returns {Promise} Resolves when operation completes
     */
    async function unshift(name, val, batch) {
      return new Promise(async (done) => {
        if (['ix', 'doc'].includes(name)) throw new Error(errProtected); // Prevent overwriting protected names
        if (![undefined, 'array'].includes(y.ix[name])) throw new Error(errArrayExpected); // limit this function to use with arrays
        const operation = async () => {
          if (!h[name]) await array(name); // create the array if it doesn't exist
          h[name].unshift(val); // modify
          c[name] = h[name].toJSON(); // Update the cache
        };
        if (batch) batched.push(operation);
        else {
          const update = Y.encodeStateAsUpdate(y.doc); // Encode the update
          if (options.leveldb) await p.storeUpdate(options.leveldb, update); // Persist the update
          await propagate({ update }); // Broadcast the update to the network
        }
        done();
      });
    }

    /**
     * Removes elements from the named array
     * @param {string} name - Array identifier
     * @param {number} index - Starting position to remove from
     * @param {number} length - Number of elements to remove
     * @returns {Promise} Resolves when operation completes
     */
    async function cut(name, index, length, batch) {
      return new Promise(async (done) => {
        if (['ix', 'doc'].includes(name)) throw new Error(errProtected); // Prevent overwriting protected names
        if (![undefined, 'array'].includes(y.ix[name])) throw new Error(errArrayExpected); // limit this function to use with arrays
        const operation = async () => {
          if (!h[name]) await array(name); // create the array if it doesn't exist
          h[name].delete(index, length); // modify
          c[name] = h[name].toJSON(); // Update the cache
        };
        if (batch) batched.push(operation);
        else {
          const update = Y.encodeStateAsUpdate(y.doc); // Encode the update
          if (options.leveldb) await p.storeUpdate(options.leveldb, update); // Persist the update
          await propagate({ update }); // Broadcast the update to the network
        }
        done();
      });
    }


    const observers = new WeakMap(); // Stores observer callbacks

    function observe(name, key, func) {
      let target;
      if (typeof key === 'function') {
        func = key;
        target = h[name];
      }
      else {
        target = h[name]?.[key];
      }
      if (!target) return false;
      const observer = (event, transaction) => {
        func(event, transaction);
      };
      target.observe(observer);
      observers.set(func, observer); // Store mapping
      return true;
    }

    function unobserve(name, key, func) {
      let target;
      if (typeof key === 'function') {
        func = key;
        target = h[name];
      }
      else {
        target = h[name]?.[key];
      }
      if (!target) return false;
      const observer = observers.get(func);
      if (observer) {
        target.unobserve(observer);
        observers.delete(func);
        return true;
      }
      return false;
    }

    
    // Create a new object with no prototype inheritance
    const api = Object.create(null, {
      isYpearCRDT: { 
        value: true,
        enumerable: true,
        configurable: true
      },
      // Define c as a getter property that returns a frozen copy of the cache
      c: {
        get() { return Object.freeze({ ...c }) }
      },
      // Define non-enumerable method properties
      map: { value: map },  // Method to get/create new maps
      set: { value: set },  // Method to set values in maps
      del: { value: del },  // Method to delete values from maps
      array: { value: array },  // Method to get/create new arrays
      insert: { value: insert }, // Method to insert content into arrays
      push: { value: push }, // Method to put a value to the back of an array
      unshift: { value: unshift }, // Method to put a value at the front of an array
      cut: { value: cut }, // Method to remove a index from an array
      propagate: { value: propagate },
      execBatch: {value: execBatch },
      onData: { value: onData },
      observe: { value: observe },
      unobserve: { value: unobserve }
    });

    // Add proxy to handle direct property access
    const proxy = new Proxy(api, {
      get(target, prop) {
        if (prop in target) return target[prop];
        return target.c[prop]; // Return cached data for direct property access
      }
    });

    // Custom object representation that shows only the cache contents
    Object.defineProperty(api, Symbol.for('nodejs.util.inspect.custom'), {
      value: function() { return this.c }
    });


    // Return the API object with controlled access to internal state
    resolve(proxy);

  });
};

module.exports = ypearCRDT;
