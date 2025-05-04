const amqplib = require('amqplib')

class RabbitMQ {
    #connection
    #channel
    #connected

    async connect(){
        if(this.#connected && this.#channel) return;
        this.#connected = true

        try {
            console.log('Connecting to RabbitMQ consumer server...')
            this.#connection = await amqplib.connect('amqp://localhost')
            console.log('RabbitMQ consumer server connection is ready...')
            this.#channel = await this.#connection.createChannel()
            console.log('RabbitMQ consumer channel was created successfully...')
        } catch (error) {
            throw error
        }
    }

    async consumeMessage(){
        /** If channel is not found, create a new connection and channel */
        if(!this.#channel) await this.connect()
        
        /** Create broker exchange.
         * This is optional as a default exchange will be created if it is not specified 
         */
        // await this.#channel.assertExchange('basic', 'topic')

        /** Define a queue. 
         * Note that we declare the queue here, as well. Because we might start the consumer before the publisher, we want to make sure the queue exists before we try to consume messages from it.
         * The 'durabe: false' ensures that exchange will not survive a broker restart. If rabbitMQ is restarted, the exchange will be deleted
         * Use 'durable: true' for persistent exchange
         **/
        const queue = 'basic_queue'
        await this.#channel.assertQueue(queue, {
            durable: false
        })

        /** This publishes the message to the queue */
        this.#channel.consume(queue, (msg) => {
            if(msg !== null){
                console.log(`Received ${msg.content.toString()} in consumer`)
            }
            /** Acknowledges the message so it can remove from queue (optional)
             * If it is not passed, the message will still be in the queue even after the consumer consumes it.
             */
            this.#channel.ack(msg)
        }, {
            noAck: false
        })

        /** Close connection after 5 seconds */
        // setTimeout(() => {
        //     this.#connection.close()
        // }, 5000)
    }
}

const consumer = new RabbitMQ()
consumer.consumeMessage()