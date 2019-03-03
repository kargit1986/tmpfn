const { PubSub } = require('@google-cloud/pubsub');
const pubsub = new PubSub();
function publish(msg, topicName) {
    const msgBuffer = Buffer.from(msg);
    return pubsub.topic(topicName).publish(msgBuffer);
}
module.exports.publish = publish;
