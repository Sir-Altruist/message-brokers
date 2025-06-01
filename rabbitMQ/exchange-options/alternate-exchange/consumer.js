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
        await this.#channel.assertExchange('alt_exchange', 'fanout', { durable: true })
        await this.#channel.assertExchange('main_exchange', 'direct', {
            durable: true,
            arguments: {
                'alternate-exchange': 'alt_exchange'
            }
        })

        /** Create and bind alternate exchange queue */
        await this.#channel.assertQueue('alt-exchange-queue', {
            durable: true
        })
        /** bind queue to an exchange */
        await this.#channel.bindQueue('alt-exchange-queue', 'alt_exchange', '')

        /** This publishes the message to the queue */
        this.#channel.consume('alt-exchange-queue', (msg) => {
            if(msg !== null){
                console.log(`Alternate exchange received a new message: ${msg.content.toString()}`)
            }
        }, {
            noAck: true
        })


        /** Create and bind main exchange queue
         * If the routing key matches and there's no issue whatsoever, 
         * it will route to the main exchange instead of the alternate exchange
         */
        await this.#channel.assertQueue('main-exchange-queue', {
            exclusive: true
        })
        /** bind queue to an exchange. */
        await this.#channel.bindQueue('main-exchange-queue', 'main_exchange', 'test')

        /** This consumes the messages from the queue */
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