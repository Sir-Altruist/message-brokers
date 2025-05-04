const amqplib = require('amqplib')

/** 
 */
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

    async publishMessage(task, time){
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
        const queue = 'task_queue'
        // const msg = process.argv.slice(2).join(' ') || "Hello World!";
        const msg = `Handling task number ${task} in ${time} milliseconds`;


        await this.#channel.assertQueue(queue, {
            durable: true
        })

        /** This publishes the message to the queue */
        this.#channel.sendToQueue(queue, Buffer.from(JSON.stringify(msg)), {
            persistent: true
        })
        console.log(`Sent ${JSON.stringify(msg)} from publisher to queue ${queue} using default exchange`)
    }
}

const producer = new RabbitMQ()
for(let i = 1; i <= 10; i++){
    /** Assuming the publisher pushes to the queue in 1 - 3 seconds consistently */
    const time = parseInt(`${Math.floor(Math.random() * 3) + 1}000`);
    setTimeout(() => producer.publishMessage(i, time), time)
}