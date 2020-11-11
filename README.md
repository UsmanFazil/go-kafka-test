# go-kafka-test
Simple kafka producer and consumer function implementation. 

"push" function writes a new message in the provided kafka topic. 
When we hit /ping endpoint, "push" function is called. 

While "consumerfunc" is a consumer to listen on the provided topic for new messages.
