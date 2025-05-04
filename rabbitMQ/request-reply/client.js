const amqplib = require('amqplib')
const { v4 } = require('uuid')

class Client {
    #connection
    #channel
    #connected

    async connect(){
        if(this.#connected && this.#channel) return;
        this.#connected = true

        try {
            console.log('Connecting to RabbitMQ client...')
            this.#connection = await amqplib.connect('amqp://localhost')
            console.log('RabbitMQ client connection is ready...')
            this.#channel = await this.#connection.createChannel()
            console.log('RabbitMQ client was created successfully...')
        } catch (error) {
            throw error
        }
    }

    async handleClient(){
        /** If channel is not found, create a new connection and channel */
        if(!this.#channel) await this.connect()

        const correlationId = v4()
        /** Reply queue */
        const replyQueue = await this.#channel.assertQueue('', {
            exclusive: true
        })
        /** This consumes the reply from the reply queue. The last step */
        this.#channel.consume(replyQueue.queue, (msg) => {
            console.log(`Received reply from server queue: ${msg.content.toString()}`);
        }, {
            noAck: true
        })

        /** Request Queue */
        /** This publishes the request to the request queue. The is the first step */
        console.log(`Sending request with cor_id: ${correlationId}`)
        const message = "Can I request a reply?"
        const requestQueueName = 'request-queue'
        this.#channel.publish('', requestQueueName, Buffer.from(message), {
            replyTo: replyQueue.queue,
            correlationId
        })
    }
}

const client = new Client()
client.handleClient()