let AWS = require('aws-sdk');
let sqs = new AWS.SQS();
let date = require('date-and-time');
const ddb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

exports.handler = (event, context, callback) => {
    
    sqs.receiveMessage({
        QueueUrl: '<<queue_url>>',  // URL of your queue
        AttributeNames: ['All'],
        MaxNumberOfMessages: '10',
        VisibilityTimeout: '30',
        WaitTimeSeconds: '20'
    }).promise()
        .then(data => {
            data.Messages.forEach(message => {      // Going through all the fetched messages in this attempt
                console.log("Received message with payload", message.Body);

                let messageBody = JSON.parse(message.Body);
                
                
                    ddb.put({
                        TableName: '{tablename}',
                        Item: {
                            
                        }
                    }).promise()
                        .then(data => {
                            console.log("Successfully inserted booking ref : " + messageBody.bookingRef +
                                " to DynamoDB with response : " + JSON.stringify(data));
                        })
                        .catch(err => {
                            console.log("Error while inserting data to DynamoDB due to : ", err);
                        });
                
				// Deleting process message to make sure it's not processed again
                sqs.deleteMessage({                         
                    QueueUrl: "<<queue_url>>",  
                    ReceiptHandle: message.ReceiptHandle

                }).promise()
                    .then(data => {
                        console.log("Successfully deleted message with ReceiptHandle : " + message.ReceiptHandle +
                            "and booking reference : " + messageBody.bookingRef + " with response :" + JSON.stringify(data));
                    })
                    .catch(err => {
                        console.log("Error while deleting the fetched message with ReceiptHandle : " + message.ReceiptHandle +
                            "and booking reference : " + messageBody.bookingRef, err);
                    });

            });
        })
        .catch(err => {
            console.log("Error while fetching messages from the sqs queue", err);
        });


    callback(null, 'Lambda execution completed');
};