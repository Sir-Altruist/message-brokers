const amqplib = require('amqplib')

class RabbitMQ {
    #connection
    #channel
    #connected

    async connect(){
        if(this.#connected && this.#channel) return;
        this.#connected = true

        try {
            console.log('Connecting to RabbitMQ payments consumer server...')
            this.#connection = await amqplib.connect('amqp://localhost')
            console.log('RabbitMQ payments consumer server connection is ready...')
            this.#channel = await this.#connection.createChannel()
            console.log('RabbitMQ payments consumer channel was created successfully...')
        } catch (error) {
            throw error
        }
    }

    async consumeMessage(){
        /** If channel is not found, create a new connection and channel */
        if(!this.#channel) await this.connect()
        
        // const exchangeName = 'pubsub-direct'
        const exchangeName = 'pubsub-topic'
        const routingKey = '#.payments' // any key ending with payments will be routed here
        // await this.#channel.assertExchange(exchangeName, 'direct')
        await this.#channel.assertExchange(exchangeName, 'topic')

        /** Define a queue. 
         * Let the system automatically generate a random queue name for us
         * The 'exclusive:true' option tells the consumer to delete the queue once the broker server connection closes
         **/
        const queue = await this.#channel.assertQueue('', {
            exclusive: true
        })

        /** bind queue to an exchange */
        await this.#channel.bindQueue(queue.queue, exchangeName, routingKey)

        /** This consumes messages from the queue */
        this.#channel.consume(queue.queue, (msg) => {
            if(msg !== null){
                console.log(`Payments service received ${msg.content.toString()} from publisher`)
            }
            /** Acknowledges the message so it can remove from queue (optional)
             * If it is not passed, the message will still be in the queue even after the consumer consumes it.
             */
            this.#channel.ack(msg)
        }, {
            noAck: false
        })
    }
}

const consumer = new RabbitMQ()
consumer.consumeMessage()