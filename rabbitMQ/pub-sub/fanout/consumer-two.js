const amqplib = require('amqplib')

class RabbitMQ {
    #connection
    #channel
    #connected

    async connect(){
        if(this.#connected && this.#channel) return;
        this.#connected = true

        try {
            console.log('Connecting to RabbitMQ consumer two server...')
            this.#connection = await amqplib.connect('amqp://localhost')
            console.log('RabbitMQ consumer two server connection is ready...')
            this.#channel = await this.#connection.createChannel()
            console.log('RabbitMQ consumer two channel was created successfully...')
        } catch (error) {
            throw error
        }
    }

    async consumeMessage(){
        /** If channel is not found, create a new connection and channel */
        if(!this.#channel) await this.connect()
        
        const exchangeName = 'pubsub-fanout'
        await this.#channel.assertExchange(exchangeName, 'fanout')

        /** Define a queue. 
         * Let the system automatically generate a random queue name for us
         * The 'exclusive:true' option tells the consumer to delete the queue once the broker server connection closes
         **/
        const queue = await this.#channel.assertQueue('', {
            exclusive: true
        })

        /** bind queue to an exchange */
        await this.#channel.bindQueue(queue.queue, exchangeName, '')

        /** This publishes the message to the queue */
        this.#channel.consume(queue.queue, (msg) => {
            if(msg !== null){
                console.log(`Consumer two received ${msg.content.toString()} from publisher`)
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