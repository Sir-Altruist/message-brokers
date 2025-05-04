const amqplib = require('amqplib')
const { v4 } = require('uuid')

class Server {
    #connection
    #channel
    #connected

    async connect(){
        if(this.#connected && this.#channel) return;
        this.#connected = true

        try {
            console.log('Connecting to RabbitMQ server...')
            this.#connection = await amqplib.connect('amqp://localhost')
            console.log('RabbitMQ server connection is ready...')
            this.#channel = await this.#connection.createChannel()
            console.log('RabbitMQ server channel was created successfully...')
        } catch (error) {
            throw error
        }
    }

    async handleServer(){
        /** If channel is not found, create a new connection and channel */
        if(!this.#channel) await this.connect()

        const correlationId = v4()
        /** Reply queue */
        const replyQueue = await this.#channel.assertQueue('request-queue', {
            exclusive: true
        })
        /** This consumes the message from the request queue. The second step */
        this.#channel.consume('request-queue', (msg) => {
            console.log(`Request received: ${msg.properties.correlationId}`);
            /** This publishes the reply back to the client server through the reply queue. The third step */
            this.#channel.publish('', msg.properties.replyTo, Buffer.from(`Hey! It's your reply to ${msg.properties.correlationId}`), {
                replyTo: replyQueue.queue,
                correlationId
            })

        }, {
            noAck: true
        })
    }
}

const server = new Server()
server.handleServer()