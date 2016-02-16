# compare reliable/not processing
---
consume message from kafka -> split into row -> insert to mongodb

---

not store message cache, emit without id, consume all message once in spout.nextTuple:
---


===

not store message cache, emit without id:
---
spend 186.475608826 seconds processing 1000000 records  
spend 186.987827063 seconds processing 1000000 records  
spend 188.370844841 seconds processing 1000000 records  

===

not store message cache, emit with id, do nothing in spout.ack / spout.fail:  
---
spend 271.760061979 seconds processing 1000000 records  
spend 270.133061886 seconds processing 1000000 records  
spend 271.751337051 seconds processing 1000000 records  
spend about 1.5 times time

===

store message cache, emit with id, delete cache in spout.ack, re-emit message in spout.fail:
---
spend 275.782381058 seconds processing 1000000 records  

===