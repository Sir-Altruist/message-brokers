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

        // const exchangeName = 'pubsub-direct'
        const exchangeName = 'pubsub-topic'
        // const paymentsRoutingKey = 'paymentsOnly' // for direct exchange
        // await this.#channel.assertExchange(exchangeName, 'direct')
        await this.#channel.assertExchange(exchangeName, 'topic')
        
        const user_payment_message = "A european user paid for something"
        const business_order_message = "A european user order goods"
        const paymentsRoutingKey = 'users.europe.payments' // for direct exchange
        const ordersRoutingKey = 'business.europe.orders' // for direct exchange
        this.#channel.publish(exchangeName, paymentsRoutingKey, Buffer.from(user_payment_message))
        this.#channel.publish(exchangeName, ordersRoutingKey, Buffer.from(business_order_message))
        console.log(`Sent messages from publisher using custom topic exchange`)

        /** Close connection after 5 seconds */
        setTimeout(() => {
            this.#connection.close()
        }, 5000)
    }
}

const producer = new RabbitMQ()
producer.publishMessage()