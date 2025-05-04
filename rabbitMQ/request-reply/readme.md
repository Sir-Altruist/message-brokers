Reply-request in rabbitMQ aloows us to send a request to a server and also specifying the server to send the reply to. In this case, we don't use the term **publisher** and **consumer**. Instead, we use **client** and **server** as both servers are producing and consuming messages (two-way communication)


- **Client**: This is what wants to sends a request to the server and receive a reply

- **Server**: This is what needs to process that request and send a reply

- **ReplyTo**: This property is passed by the client to the exchange along with the message. It tells the server the specific queue to reply to. 

- **correlationId**: In a situation where the client sends multiple requests to a request queue and are processed by the server or perhaps more than one server. It then receives multiple replies in the reply queue. This **correlationId** metadata is used to know which reply belongs to which request. The client tags the metadata along with the request to the request queue. The server also tags the metadata on the reply along with the response