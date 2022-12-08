const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const topicName = 'orderCreated';
const consumerNumber = process.argv[2] || '1';

const processConsumer  = async () => {
    // adding consumers to 3 different consumer groups
    const ordersConsumer = kafka.consumer({groupId: 'orders'});
    const paymentsConsumer = kafka.consumer({groupId: 'payments'});
    const notificationsConsumer = kafka.consumer({groupId: 'notifications'});
    await Promise.all([
        ordersConsumer.connect(),
        paymentsConsumer.connect(),
        notificationsConsumer.connect(),
    ]);

    // subscribe the 3 consumers to the topic
    await Promise.all([
        await ordersConsumer.subscribe({ topic: topicName }),
        await paymentsConsumer.subscribe({ topic: topicName }),
        await notificationsConsumer.subscribe({ topic: topicName }),
    ]);

    // start the consumers (with .run) to enable them to listen to any messages that are published on their
    // respective subscribed topics
    let orderCounter = 1;
    let paymentCounter = 1;
    let notificationCounter = 1;
    await ordersConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            logMessage(orderCounter, `ordersConsumer#${consumerNumber}`, topic, partition, message);
            orderCounter++;
        },
    });
    await paymentsConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            logMessage(paymentCounter, `paymentsConsumer#${consumerNumber}`, topic, partition, message);
            paymentCounter++;
        },
    });
    // await notificationsConsumer.run({
    //     eachMessage: async ({ topic, partition, message }) => {
    //         logMessage(notificationCounter, `notificationsConsumer#${consumerNumber}`, topic, partition, message);
    //         notificationCounter++;
    //     },
    // });
    // re-introduce an error for to force retries on kafka
    await notificationsConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            throw new Error('some error got in the way which didnt let the message be consumed successfully');
            logMessage(notificationCounter, `notificationsConsumer#${consumerNumber}`, topic, partition, message);
            notificationCounter++;
        },
    });

};

const logMessage = (counter, consumerName, topic, partition, message) => {
    console.log(`received a new message number: ${counter} on ${consumerName}: `, {
        topic,
        partition,
        message: {
            offset: message.offset,
            headers: message.headers,
            value: message.value.toString()
        },
    });
};

processConsumer();