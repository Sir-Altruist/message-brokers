const amqplib = require('amqplib')

class RabbitMQ {
    #connection
    #channel
    #connected

    async connect(){
        if(this.#connected && this.#channel) return;
        this.#connected = true

        try {
            console.log('Connecting to RabbitMQ producer server...')
            this.#connection = await amqplib.connect('amqp://localhost')
            console.log('RabbitMQ product server connection is ready...')
            this.#channel = await this.#connection.createChannel()
            console.log('RabbitMQ producer channel was created successfully...')
        } catch (error) {
            throw error
        }
    }

    async publishMessage(){
        /** If channel is not found, create a new connection and channel */
        if(!this.#channel) await this.connect()
        
        /** Create broker exchange.
         * This is optional as a default exchange will be created if it is not specified 
         */
        // await this.#channel.assertExchange('basic', 'topic')

        /** Define a queue. 
         * This ensures that the queue the consumer will be reading from exists
         * The 'durabe: false' ensures that exchange will not survive a broker restart. If rabbitMQ is restarted, the exchange will be deleted
         * Use 'durable: true' for persistent exchange
         **/
        const queue = 'basic_queue'
        const msg = { name: 'Altruistic' }
        await this.#channel.assertQueue(queue, {
            durable: false
        })

        /** This publishes the message to the queue */
        this.#channel.sendToQueue(queue, Buffer.from(JSON.stringify(msg)))
        console.log(`Sent ${JSON.stringify(msg)} from publisher to queue ${queue} using default exchange`)

        /** Close connection after 5 seconds */
        setTimeout(() => {
            this.#connection.close()
        }, 5000)
    }
}

const producer = new RabbitMQ()
producer.publishMessage()