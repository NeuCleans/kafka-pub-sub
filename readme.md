# Kafka Pub Sub


Pub Sub wrapper around [kafka-node](https://github.com/SOHU-Co/kafka-node)

## Getting Started


### Prerequisites

* [kafka](https://github.com/NeuCleans/kafka-docker/tree/pub-sub)

### Setup

`yarn add https://github.com/NeuCleans/kafka-pub-sub.git`


### Usage

### Producer/Consumer

```
async function sampleKafkaPubSub() {
    const topic = 'SOME_TOPIC_ID'

    ServiceProducer.Logger = new Logger();
    ServiceProducer.SERVICE_ID = SERVICE_ID;

    await ServiceConsumer.subscribe(topic);
    ServiceConsumer.listen((message) => {
        console.log(`Message: ${JSON.stringify(message, null, 2)}`);
    });

    setInterval(async () => {
        console.log('sending....');
        const msg = await ServiceProducer.buildAMessageObject({ date: `${new Date().toISOString()}` }, topic);
        try {
            await ServiceProducer.send([msg]);
        } catch (error) {
            console.error(error.stack);
        }
    }, 5 * 1000);
}
```

### HLProducer/ConsumerGroup

```
async function sampleKafkaPubSubHL() {
    const topic = 'SOME_TOPIC_ID'

    ServiceHLProducer.Logger = new Logger();
    ServiceHLProducer.SERVICE_ID = SERVICE_ID;

    await ServiceConsumerGroup.subscribe(topic);
    ServiceConsumerGroup.listen((message) => {
        console.log(`Message: ${JSON.stringify(message, null, 2)}`);
    });

    setInterval(async () => {
        console.log('sending....');
        const msg = await ServiceHLProducer.buildAMessageObject({ date: `${new Date().toISOString()}` }, null, topic);
        try {
            await ServiceHLProducer.send([msg]);
        } catch (error) {
            console.error(error.stack);
        }
    }, 5 * 1000);
}
```

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.


## Authors

* **Claudius Mbemba** - *Initial work* - [User1m](https://github.com/User1m)

See also the list of [contributors](https://github.com/NeuCleans/kafka-pub-sub/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

