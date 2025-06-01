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

        /** Create the main and alternative exchange */
        await this.#channel.assertExchange('alt_exchange', 'fanout', { durable: true })
        await this.#channel.assertExchange('main_exchange', 'direct', {
            durable: true,
            arguments: {
                'alternate-exchange': 'alt_exchange'
            }
        })

        const msg = "Hello! this is my first message"
        this.#channel.publish('main_exchange', 'test', Buffer.from(JSON.stringify(msg)))
        console.log(`Sent ${JSON.stringify(msg)} from publisher`)

        /** Close connection after 5 seconds */
        setTimeout(() => {
            this.#connection.close()
        }, 5000)
    }
}

const producer = new RabbitMQ()
producer.publishMessage()