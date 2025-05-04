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
        const queue = 'task_queue'
        await this.#channel.assertQueue(queue, {
            durable: true
        })

        // Limit unacknowledged messages to 1 per consumer
        this.#channel.prefetch(1); // This prevents consumers from having backlogs of task queues i.e round-robin

        console.log("Waiting for messages...");

        /** This consumes the message from the queue */
        this.#channel.consume(queue, async (msg) => {
            if(msg !== null){
                const content = msg.content.toString();
                console.log("Received:", content);

                // Simulate some work
                /** Assuming it takes 1 - 5 seconds for consumer to finish a task */
                const time = parseInt(`${Math.floor(Math.random() * 5) + 1}000`);
                console.log('consumer time: ', time)
                await new Promise(resolve => setTimeout(resolve, time));
                console.log("Done processing:", content);

                this.#channel.ack(msg)

            }
        }, {
            noAck: false // required if you're acknowledging manually
        })
    }
}

const consumer = new RabbitMQ()
consumer.consumeMessage()