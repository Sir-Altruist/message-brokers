const amqplib = require('amqplib')
const prompt = require('prompt-sync')()

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

        const exchangeName = 'accept_or_reject_exchange'
        await this.#channel.assertExchange(exchangeName, 'fanout')

        const msg = "Let's send this message"

        let count = 0
        while(count < 3){
            this.#channel.publish(exchangeName, '', Buffer.from(JSON.stringify(msg)))
            console.log(`Sent ${JSON.stringify(msg)} from publisher`)
            prompt('Press any key to continue...')
            count++
        }

    }
}

const producer = new RabbitMQ()
producer.publishMessage()