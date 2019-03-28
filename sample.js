const uuidByString = require("uuid-by-string");
const kafkaPS = require('kafka-pub-sub');

const kHostCluster = 'localhost:32809,localhost:32810,localhost:32811';
// const kHostCluster = 'localhost:9094,localhost:9096,localhost:9010';
// const kHostCluster = 'localhost:9092';

function sampleKafkaPubSub() {
    const topic = uuidByString('jane@doe.com');
    kafkaPS.ServiceConsumer.init(topic, kHostCluster)
        .then(() => {
            // kafkaPS.ServiceConsumer.listen((message) => {//message is automatically logged
            //     console.log(JSON.stringify(message, null, 2));
            //     kafkaPS.ServiceConsumer.commit();
            // }, false); //DON't auto commit after each message. I will handle that.

            kafkaPS.ServiceConsumer.listen((message) => {
                //message is automatically logged
            }); //auto commit after each message

            kafkaPS.ServiceConsumer.onError((error) => {
                console.log(`!!Error: ${JSON.stringify(error, null, 2)}`);
            }, true);

            // kafkaPS.ServiceProducer.init(topic, kHostCluster).then(() => {
            setInterval(() => {
                console.log('sending....');
                kafkaPS.ServiceProducer.buildAMessageObject({ date: `${new Date().toISOString()}` }, topic)
                    .then((msg) => kafkaPS.ServiceProducer.send([msg]))
                    .catch(error => console.error(error.stack));
            }, 5 * 1000);
            // });

        });
}

function sampleKafkaPubSubHL() {
    const topic = uuidByString('john@doe.com');
    const topicOpts = {
        partitions: 10,
        replicationFactor: 3
    }

    kafkaPS.ServiceConsumerGroup.init(topic, topicOpts, { kafkaHost: kHostCluster })
        .then(() => {
            kafkaPS.ServiceConsumerGroup.listen((message) => {
                //message is automatically logged
            });

            kafkaPS.ServiceConsumerGroup.onError((error) => {
                console.log(`!!Error: ${JSON.stringify(error, null, 2)}`);
            });
        })
        .then(() => {
            // kafkaPS.ServiceHLProducer.init(topic, topicOpts, kHostCluster).then(() => {
            setInterval(() => {
                console.log('sending....');
                kafkaPS.ServiceHLProducer.buildAMessageObject({ date: `${new Date().toISOString()}` }, topic)
                    .then((msg) => kafkaPS.ServiceHLProducer.send([msg]))
                    .catch(error => console.error(error.stack));
            }, 5 * 1000);
            // })
        });
}

sampleKafkaPubSub();
// sampleKafkaPubSubHL();