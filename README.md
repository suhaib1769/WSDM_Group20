# Web-scale Data Management Project 
 
## Solution using 2 Phase Commit + 2 Phase Locking : 
consistent, fault tolerant but not scalable

## Implementation 
- For our application, we implement a 2 Phase Commit Protocol along with 2 Phase Locking. 
- The order service acts as the coordinator.
- On receiving a checkout request, the order service databse is locked.
- **Prepare Phase** : 
    - Order Service calls the Stock service to check if it has enough stock and the Payment service to check if the user has enough credits to process the order
    - If the stock service does not have sufficient stock or if the user does not have sufficient credits (400 status), the transaction is cancelled and the locks are released.
    - If both services return a 200 status(success), the commit phase is implemented. 
- **Commit Phase** : 
    - In the commit phase, Order services sends a call to stock and payment services to commit the changes to the database.
    - Once both the services completes the commit operations, the order service db commits the changes and releases the lock.

### Fault Tolerance
To ensure that our system is robust, we implemented recovery logging. The logs and values are written to the database as one atomic transaction. For each transaction, we generate a unique id. Each time we write to the database, the logs corresponding the transaction id are written to the database as well to keep track of the changes made so far. 


**Order Service Failure** : If the Order service fails, we store logs in the stock and payment service db corresponding to the given order. Once the order services revives, subsequent checkout calls using the same order would check if the stock and payments logs contain changes to the database for that specific order and subsequent actions are taken accordingly

**Stock Service Failure** : If Stock service fails, the order service waits for the stock service to recover and checks the stock service logs to track the changes written to the disk so far. For each item in the stock, we check if there are any write operations done for the item and order combination in the stock service database. If there are, we return a 200 status back to stock. If not, we perform the operations in the stock service and return the response.

**Payment Service Failure** : If the Payment service fails, the order service waits for the payment service to recover and checks the payment service logs to track the changes written to the disk so far. If there are any write operations done corresponding to the order in the payment service database, we return a 200 status back to stock. If not, we perform the operations in the payment service and return the response.


For faut tolerance, we made the following assumptions: 
- Once a service is down, it would be revived after a while.  
- On failing a service, any API calls made to the service would return a "502 Gateway Not Found" response
=

## What works :
- Atomic transactions for each checkout
- Prevents dirty reads
- Consistent and perfoms well in the consistency test. 

## Limitations of our method: 
- Not Scalable
- Failure of the Coordinator Service could impact the protocol


### Other architectures that we tried to implement

1. **Synchronous orchestrated SAGAS with rabbitmq - {branch name}**
- Used RabbitMQ as the message bus between the services
- Set up a separate consumer (as a service) for the order service
- Process:
    - Order service would publish a request onto the stock-queue or the payment-queue which is consumed by the stock or payment service respectively. 
    - The stock/payment services processes the request and publishes the response to the order-consumer service where it would constantly consume messages enqueue into a separate response_queue for the payment and stock responses. 
    - The order service would then send an HTTP request to order-consumer service and dequeue the requests from the response_queue and use it for further processing. 
### Issues faced
- The order-consumer would sometimes get the http request from the order service before it would have even consumed the responses from the stock and payment services. We decided to loop the HTTP requests from the order service until it is able to dequeue a proper response. This would effectively block the order-service from taking in new requests, resulting in a timeout during the consistency test. Our implementation is therefore slow and not scalable.  

2. **Asynchronous orchestrated SAGAS with AIOKafka and Quart - {branch name}**
### What we implemented
- Used AIOKafka as the message bus between the services
- To make it asychronous, we used Quart Flask and aioKafka
- The 
### Issues faced
- The quart app of the stock had continuous consumer and when we needed to consume additional external HTTP requests

### Testing Instructions

#### docker-compose (local development)

- run `docker-compose up --build` in the base folder of to test
- 

***Requirements:*** You need to have docker and docker-compose installed on your machine.
