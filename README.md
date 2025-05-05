# <img src="https://github.com/ypear/crdt/blob/main/Yjs.png" height="32" style="vertical-align:40px;"/>🍐@ypear/crdt.js 📑

### 💾 Installation

```bash
npm install @ypear/crdt
```

### 👀 Description

A Conflict-Free Replicated Data Type (CRDT) implementation using Yjs with Hyperswarm networking and optional LevelDB persistence.

### 🤯 Gotchas:

- Distributed synchronization via Hyperswarm network

- Optional persistence using LevelDB

- Map, Array and Array nested in Maps support with automatic conflict resolution

- Efficient delta updates with state vector tracking

- `options.observerFunction` allows you to receive real-time updates

- Access current state directly via `crdt.c` (cache) property


### ✅ Usage
```javascript
(async () => {
  const router = await require('@ypear/router')(peers, {
    networkName: 'very-unique-4898n0aev7e7egigtr',
    seed: '76e78c7efa235f018799125822feb0c4', // example only 32 hex (generate a different one)
    username: 'alice' // example only username
  });
  const crdt = await require('@ypear/crdt')(router, {
    topic: 'my-crdt-topic',
    leveldb: './my-database', // optional persistence
    observerFunction: (update) => {
      // Handle real-time updates
    }
  });

  // Map operations
  await crdt.map('users');
  await crdt.set('users', 'user1', {name: 'Alice'});
  await crdt.del('users', 'user1');

  // Array operations
  await crdt.array('messages');
  await crdt.push('messages', 'Hello');
  await crdt.unshift('messages', 'Hello');
  await crdt.insert('messages', 0, ['Welcome']);
  await crdt.cut('messages', 0, 1);

  // Batch operations

  // Batch Map operations
  await crdt.map('users', true);
  await crdt.set('users', 'user1', {name: 'Alice'}, true);
  await crdt.del('users', 'user1', true);

  // Batch Array operations
  await crdt.array('messages', true);
  await crdt.push('messages', 'Hello', true);
  await crdt.unshift('messages', 'Hello', true);
  await crdt.insert('messages', 0, ['Welcome'], true);
  await crdt.cut('messages', 0, 1, true);

  await crdt.execBatch();

  // todo: Array inside a Map


})();
```


### 🧰 Methods

- `map(name)`: Creates/gets a CRDT map
- `set(mapName, key, value)`: Sets a value in a map
- `get(mapName, key)`: Gets a value from a map
- `del(mapName, key)`: Deletes a key from a map
- `array(name)`: Creates/gets a CRDT array
- `push(arrayName, value)`: Appends to an array
- `insert(arrayName, index, value)`: Inserts into an array
- `cut(arrayName, index, length)`: Removes from an array
- `execBatch(operations)`: Executes batched operations atomically


## 📜 License
MIT
