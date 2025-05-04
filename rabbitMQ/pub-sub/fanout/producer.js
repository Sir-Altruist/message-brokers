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

        const exchangeName = 'pubsub-fanout'
        await this.#channel.assertExchange(exchangeName, 'fanout')

        const msg = { name: 'Altruistic' }
        this.#channel.publish(exchangeName, '', Buffer.from(JSON.stringify(msg)))
        console.log(`Sent ${JSON.stringify(msg)} from publisher using custom fanout exchange`)

        /** Close connection after 5 seconds */
        setTimeout(() => {
            this.#connection.close()
        }, 5000)
    }
}

const producer = new RabbitMQ()
producer.publishMessage()