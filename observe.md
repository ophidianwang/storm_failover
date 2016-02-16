# compare reliable/not processing
---
consume message from kafka -> split into row -> insert to mongodb

---

not store message cache, emit without id, consume & emit all message at once in spout.nextTuple:
---
spout spend 88.0975561142 seconds processing 1000000 records  
spout spend 89.1499040127 seconds processing 1000000 records  
spout spend 87.5488100052 seconds processing 1000000 records  

===

not store message cache, emit without id, consume & emit 1000 messages at once in spout.nextTuple:
---
spout spend 95.9106609821 seconds processing 1000000 records  
spout spend 92.1038460732 seconds processing 1000000 records  
spout spend 91.3895950317 seconds processing 1000000 records  

===

not store message cache, emit without id, consume & emit one message in spout.nextTuple:
---
spout spend 186.475608826 seconds processing 1000000 records  
spout spend 186.987827063 seconds processing 1000000 records  
spout spend 188.370844841 seconds processing 1000000 records  
spend about 2.2 times of time; 100 seconds more, compare to emit all message in once spout.nextTuple

===

not store message cache, emit with id, do nothing in spout.ack / spout.fail:  
---
spout spend 271.760061979 seconds processing 1000000 records  
spout spend 270.133061886 seconds processing 1000000 records  
spout spend 271.751337051 seconds processing 1000000 records  
spend about 1.5 times of time; 90 seconds more, comparing to non-reliable spout emit 

===

store message cache, emit with id, delete cache in spout.ack, re-emit message in spout.fail:
---
spout spend 275.782381058 seconds processing 1000000 records  
spout spend 275.310725927 seconds processing 1000000 records  
spout spend 274.869796038 seconds processing 1000000 records  
spend about 1/75, 4 seconds more than not store cache and do nothing in spout.ack / spout.fail

===

store message cache, emit with id, delete cache in spout.ack, re-emit message in spout.fail, consume & emit 1000 messages at once in spout.nextTuple:
---
spout spend 201.700664997 seconds processing 1000000 records  
spout spend 224.853994846 seconds processing 1000000 records  
spout spend 220.43433094 seconds processing 1000000 records  
spout spend about 2.2~2.4 times of time; 110~130 seconds more, compare to emit without id  
will fail & re-emit many times, not realistic for production  

===

store message cache, emit with id, delete cache in spout.ack, re-emit message in spout.fail, consume & emit 100 messages at once in spout.nextTuple:
---
spout spend 218.540943861 seconds processing 1000000 records (no fail)  
spout spend 230.486761808 seconds processing 1000000 records (57840 fail, become 1058412 records in mongodb)  
spout spend 220.335182905 seconds processing 1000000 records (no fail)  
spout spend 220.10848999 seconds processing 1000000 records (no fail)  

===

conclusion:
---
1. spout.nextTuple has basic cost (time); consider it  
2. emit with tuple.id (i.e. use storm reliable processing) will cost more (time)  
3. time-wasting loop in spout.nextTuple will make spout.ack / spout.fail be not executable; topology will be blocked, or cause false negative spout.fail  
