# B+Tree implementation in Kotlin

## Overview
This project is my learning project for B+Tree (also my first Kotlin experience). It is heavily inspired by InnoDB. So far the list below is implemented in total 963 lines of code:

- Basic get/put/delete/scan
- Split page at the middle
- Read page from memory if it exists, otherwise read from the file
- Write page to the file synchronously

I stopped here since I had learned a lot. The list below is lots of TODO:

- Merge pages
- Logical delete
- Split by different strategy
- Asynchronous write + WAL
- MVCC
- etc.

## Resources
- [B\+Tree index structures in InnoDB – Jeremy Cole](https://blog.jcole.us/2013/01/10/btree-index-structures-in-innodb/)
    - This is the bible for this project.
- [Part 7 \- Introduction to the B\-Tree \| Let’s Build a Simple Database](https://cstack.github.io/db_tutorial/parts/part7.html)
    - This series of posts are great to understand basic of B+Tree. 

## Schema
This B+Tree implementation is mostly generic/schema free. You can use any byte array representation for key and value, but I use Apache Avro for demo purpose since it is sortable even in byte array form (i.e. without decoding)

## Demo
First, initialize the db file:

```
$ ./runsample init foo.db
```

Then, you can `get/put/delete/scan` the B+Tree (The default schema is under `/sample` directory):

```
$ ./runsample open foo.db
COMMAND: put
KEY: {"name": "foo", "timestamp": 1}
VALUE: {"value": "bbb"}

COMMAND: get
KEY: {"name": "foo", "timestamp": 1}
{"name": "foo", "timestamp": 1} {"value": "bbb"}

COMMAND: put
KEY: {"name": "foo", "timestamp": 10}
VALUE: {"value": "bbb"}

COMMAND: scan
START KEY: {"name": "foo", "timestamp": 0}
END KEY: {"name": "foo", "timestamp": 100}
{"name": "foo", "timestamp": 1} {"value": "bbb"}
{"name": "foo", "timestamp": 10} {"value": "bbb"}

COMMAND: put
KEY: {"name": "bar", "timestamp": 10}
VALUE: {"value": "bbb"}

COMMAND: put
KEY: {"name": "bar", "timestamp": 1}
VALUE: {"value": "bbb"}

COMMAND: scan
START KEY: {"name": "foo", "timestamp": 0}
END KEY: {"name": "foo", "timestamp": 100}
{"name": "foo", "timestamp": 1} {"value": "bbb"}
{"name": "foo", "timestamp": 10} {"value": "bbb"}

COMMAND: scan
START KEY: {"name": "bar", "timestamp": 0}
END KEY: {"name": "foo", "timestamp": 100}
{"name": "bar", "timestamp": 1} {"value": "bbb"}
{"name": "bar", "timestamp": 10} {"value": "bbb"}
{"name": "foo", "timestamp": 1} {"value": "bbb"}
{"name": "foo", "timestamp": 10} {"value": "bbb"}
```

## Internal
### Tree
This is a class for B+Tree itself. It supports the operations below:

- get(key)
    1. Find appropriate LeafNode
    2. Get KeyValue from the LeafNode
- put(key, value)
    1. Find appropriate LeafNode
    2. Put KeyValue into the LeafNode and sync with the file
    3. If the Page of the LeafNode is full, split it
        - Add the new LeafNode to the same parent
    4. If any InternalNode is also full, split it
        - Add the new InternalNode to the same parent
        - Sync changed pages with the file
    5. If RootNode is full, split it
        - Add new Nodes as children
            - Left node's minimum key is logical minimum key (0 bytes)
        - Sync changed pages with the file
- delete(key)
    1. Find appropriate LeafNode
    2. Delete KeyValue from the LeafNode and sync with the file
- scan(startKey, endKey)
    1. Find appropriate LeafNode for the startKey and endKey
    2. Filter KeyValues inside the LeafNode between startKey and endKey
    3. Traverse linked list of LeafNodes until the LeafNode for endKey 

### Node
This is a (set of) class for Node of B+Tree.

#### LeafNode
This class stores all KeyValue on backend Page.

- get(key)
    1. Loop KeyValue list on Page
    2. Find exactly matching KeyValue
- put(key, value)
    1. Loop KeyValue list on Page
    2. If there is exactly matching KeyValue, update it
    3. If not, insert it with sorted order
- delete(key)
    1. Loop KeyValue list on Page
    2. Delete exactly matching KeyValue
    
#### InternalNode (extends LeafNode)
This class stores minimum key of each child and its Page id as KeyValue.

- findChildPageId(key)
    1. Loop KeyValue list on Page
    2. Find a child for the key
        - If k1 <= key < k2, return the value of k1
        - If key < k-left-edge, return the value of k-left-edge
- addChildNode(node)
    1. Fetch minimum key of the given node
    2. Put the minimum key and node id as KeyValue

#### RootNode (extends InternalNode)
This class is for the RootNode. RootNode is LeafNode for the first time, then it becomes InternalNode after the first split. So, it inherits both.

- splitRoot()
    1. Create a new Page for left LeafNode and move all KeyValue there
    2. Split the left LeafNode i.e. a new right LeafNode is created

### Page
This is a class for Page of B+Tree. Page is backend data structure for all Node classes. PageData is Avro record:

```
{
  "name": "PageData",
  "type": "record",
  "fields": [
    {"name": "id", "type": "PageId"},
    {"name": "nodeType", "type": "NodeType"},
    {"name": "previousId", "type": "PageId"},
    {"name": "nextId", "type": "PageId"},
    {"name": "records", "type": {"type": "array", "items": "KeyValue"} }
  ]
}
```

So, the Page is encoded/decoded as Avro's binary format (single-object encoding).

It supports mutational operations below and calculates encoded byte size to check whether the Page is full:

- insert(index, keyvalue)
- update(index, keyvalue)
- delete(index)

### FileManager
This class is for managing file read and write. The file has fixed size (128 bytes) metadata at the beginning, then each Page (256 bytes) is stored ordered by Page id.

- allocate()
    1. Fetch next free Page id and update it
        - So far, this is always the next of last Page. If merge is implemented, merged pages are reclaimed.
    2. Create a new Page for the id on memory
- read(id)
    1. Seek to the first byte for the Page id
    2. Read all bytes for the Page (1KB) and return decoded Page object
- write(page)
    1. Seek to the first byte for the Page id
    2. Write encoded Page

### PageManager
This class is for managing Page lifecycle. It has memory pool for pages (currently just a `Map`).

- get(id)
    1. If it is in the pool, return it
    2. If not, read from FileManager and also cache it
- allocate()
    1. Allocate a new Page from FileManager and cache it
- split(page)
    1. Find split point (currently middle of size)
    2. Delete right side KeyValue list
    3. Allocate a new Page with deleted KeyValue list
    4. Connect Linked list between Nodes
    5. Write to the file

## Not optimized split
Currently, there is only one split strategy: middle of size. This is the best approach for random insert since a new KeyValue comes random place and half split pages could be filled later.

However, if the insert is in order (both ascending and descending), it is not optimized. Because a new KeyValue is always inserted to one node, rest of nodes are left with half usage.

InnoDB has an optimization to solve this problem (insertion order). See [this great post](https://stackoverflow.com/questions/48364549/how-does-the-leaf-node-split-in-the-physical-space-in-innodb).

### Random insert
```
RootNode(id:0 size=93 records=14)
    key=
    LeafNode(id:1 size=163 keys=[00:01, 00:02, 00:03])
    key=00:04
    LeafNode(id:11 size=163 keys=[00:04, 00:05, 00:06])
    key=00:07
    LeafNode(id:7 size=209 keys=[00:07, 00:08, 00:09, 00:0a])
    key=00:0b
    LeafNode(id:9 size=163 keys=[00:0b, 00:0c, 00:0d])
    key=00:0e
    LeafNode(id:5 size=209 keys=[00:0e, 00:0f, 00:10, 00:11])
    key=00:12
    LeafNode(id:10 size=163 keys=[00:12, 00:13, 00:14])
    key=00:15
    LeafNode(id:3 size=163 keys=[00:15, 00:16, 00:17])
    key=00:18
    LeafNode(id:14 size=163 keys=[00:18, 00:19, 00:1a])
    key=00:1b
    LeafNode(id:2 size=255 keys=[00:1b, 00:1c, 00:1d, 00:1e, 00:1f])
    key=00:20
    LeafNode(id:12 size=209 keys=[00:20, 00:21, 00:22, 00:23])
    key=00:24
    LeafNode(id:4 size=163 keys=[00:24, 00:25, 00:26])
    key=00:27
    LeafNode(id:13 size=163 keys=[00:27, 00:28, 00:29])
    key=00:2a
    LeafNode(id:6 size=209 keys=[00:2a, 00:2b, 00:2c, 00:2d])
    key=00:2e
    LeafNode(id:8 size=255 keys=[00:2e, 00:2f, 00:30, 00:31, 00:32])
```

### Ascending ordered insert
``` 
RootNode(id:0 size=103 records=16)
    key=
    LeafNode(id:1 size=163 keys=[00:01, 00:02, 00:03])
    key=00:04
    LeafNode(id:2 size=163 keys=[00:04, 00:05, 00:06])
    key=00:07
    LeafNode(id:3 size=163 keys=[00:07, 00:08, 00:09])
    key=00:0a
    LeafNode(id:4 size=163 keys=[00:0a, 00:0b, 00:0c])
    key=00:0d
    LeafNode(id:5 size=163 keys=[00:0d, 00:0e, 00:0f])
    key=00:10
    LeafNode(id:6 size=163 keys=[00:10, 00:11, 00:12])
    key=00:13
    LeafNode(id:7 size=163 keys=[00:13, 00:14, 00:15])
    key=00:16
    LeafNode(id:8 size=163 keys=[00:16, 00:17, 00:18])
    key=00:19
    LeafNode(id:9 size=163 keys=[00:19, 00:1a, 00:1b])
    key=00:1c
    LeafNode(id:10 size=163 keys=[00:1c, 00:1d, 00:1e])
    key=00:1f
    LeafNode(id:11 size=163 keys=[00:1f, 00:20, 00:21])
    key=00:22
    LeafNode(id:12 size=163 keys=[00:22, 00:23, 00:24])
    key=00:25
    LeafNode(id:13 size=163 keys=[00:25, 00:26, 00:27])
    key=00:28
    LeafNode(id:14 size=163 keys=[00:28, 00:29, 00:2a])
    key=00:2b
    LeafNode(id:15 size=163 keys=[00:2b, 00:2c, 00:2d])
    key=00:2e
    LeafNode(id:16 size=255 keys=[00:2e, 00:2f, 00:30, 00:31, 00:32])
```

### Descending ordered insert
```
RootNode(id:0 size=103 records=16)
    key=
    LeafNode(id:1 size=255 keys=[00:01, 00:02, 00:03, 00:04, 00:05])
    key=00:06
    LeafNode(id:16 size=163 keys=[00:06, 00:07, 00:08])
    key=00:09
    LeafNode(id:15 size=163 keys=[00:09, 00:0a, 00:0b])
    key=00:0c
    LeafNode(id:14 size=163 keys=[00:0c, 00:0d, 00:0e])
    key=00:0f
    LeafNode(id:13 size=163 keys=[00:0f, 00:10, 00:11])
    key=00:12
    LeafNode(id:12 size=163 keys=[00:12, 00:13, 00:14])
    key=00:15
    LeafNode(id:11 size=163 keys=[00:15, 00:16, 00:17])
    key=00:18
    LeafNode(id:10 size=163 keys=[00:18, 00:19, 00:1a])
    key=00:1b
    LeafNode(id:9 size=163 keys=[00:1b, 00:1c, 00:1d])
    key=00:1e
    LeafNode(id:8 size=163 keys=[00:1e, 00:1f, 00:20])
    key=00:21
    LeafNode(id:7 size=163 keys=[00:21, 00:22, 00:23])
    key=00:24
    LeafNode(id:6 size=163 keys=[00:24, 00:25, 00:26])
    key=00:27
    LeafNode(id:5 size=163 keys=[00:27, 00:28, 00:29])
    key=00:2a
    LeafNode(id:4 size=163 keys=[00:2a, 00:2b, 00:2c])
    key=00:2d
    LeafNode(id:3 size=163 keys=[00:2d, 00:2e, 00:2f])
    key=00:30
    LeafNode(id:2 size=163 keys=[00:30, 00:31, 00:32])
```