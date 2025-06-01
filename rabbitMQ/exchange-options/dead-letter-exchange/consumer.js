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
        
        /** Create the main and alternative exchange */
        await this.#channel.assertExchange('dead_letter_exchange', 'fanout', { durable: true })
        await this.#channel.assertExchange('main_exchange', 'direct')

        /** Create and bind main exchange queue */
        await this.#channel.assertQueue('main-exchange-queue', {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': 'dead_letter_exchange',
                'x-message-ttl': 1000
            }
        })
        /** bind main queue to main exchange */
        await this.#channel.bindQueue('main-exchange-queue', 'main_exchange', 'test')

        /** create dead-letter queue */
        await this.#channel.assertQueue('dead-letter-queue')

        /** bind dead-letter queue */
        await this.#channel.bindQueue('dead-letter-queue', 'dead_letter_exchange', '')

        /** This publishes the message to the dead-letter queue after expiry */
        this.#channel.consume('dead-letter-queue', (msg) => {
            if(msg !== null){
                console.log(`Dead letter exchange received a new message: ${msg.content.toString()}`)
            }
        }, {
            noAck: true
        })

        /** This publishes the message to the queue (optional)
         * If we declare this, message will come here instead of dead letter queue as it will be consumed by the main queue before it expires
         */
        this.#channel.consume('main-exchange-queue', (msg) => {
            if(msg !== null){
                console.log(`Main exchange received a new message: ${msg.content.toString()}`)
            }
        }, {
            noAck: true
        })
    }

}

const consumer = new RabbitMQ()
consumer.consumeMessage()