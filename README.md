# Simple B+Tree implementation

## Overview
This project is my learning project for B+Tree. So far the list below is implemented:

- Split page at the middle
- Read page from memory if it exists, otherwise read from the file
- Write page to the file synchronously

I stopped here since I had learned a lot. The list below is lots of TODO:

- Merge pages
- Split by different strategy
- Asynchronous write + WAL
- MVCC
- etc.

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
    5. If RootNode is full, split it
        - Add new Nodes as children, left node's minimum key is logical minimum key (0 bytes)
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
    1. Fetch minimum key of the node
    2. Put the minimum key and node id as KeyValue

#### RootNode (extends InternalNode)
This class is for the RootNode. RootNode is LeafNode for the first time, then it becomes InternalNode after the first split. So, it inherits both.

- splitRoot()
    1. Create a new Page for left LeafNode and move all KeyValue
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
This class is for managing file read and write. The file has fixed size (512KB) metadata at the beginning, then each Page (1KB) is stored ordered by Page id.

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
Currently, there is only one split strategy: middle of size. This is the best approach for random insert since a new KeyValue comes random place and half split pages could be filled.

However, if the insert is in order (both ascending and descending), it is not optimized. Because a new KeyValue is always inserted to one node, rest of nodes are left with half usage.

InnoDB has an optimization to solve this problem. See this post.

