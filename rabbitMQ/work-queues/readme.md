## Work Queues (aka: Task Queues)
The main idea behind **Work Queues (aka: Task Queues)** is to avoid doing a **resource-intensive task** (i.e like images to be resized or pdf files to be rendered) immediately and having to wait for it to complete. Instead we schedule the task to be done later. We encapsulate a task as a message and send it to a queue. A **worker process** running in the background will pop the tasks and eventually execute the job. When you run many workers the tasks will be shared between them.
This concept is especially useful in web applications where it's impossible to handle a complex task during a short HTTP request window.

### Methods of distributing tasks 
- **Round Robin**: This method assigns tasks to consumers in a sequential order i.e ConsumerA -> Task 1, ConsumerB -> Task 2, ConsumerA -> Task 3, ConsumerB -> Task 4, etc

- **Competing Consumers**: This method assigns tasks to consumers based on loads. It checks for any available consumer and assigns the task to it instead of putting it in the backlog of a busy queue in a particular sequence.