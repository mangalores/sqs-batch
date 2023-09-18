# sqs-batch
A sqs consumer that can be configured to fetch a larger number of messages to put them together to larger message collections for more convenient behavior for batch batch processes. This helps to deal with the relatively low max message count of 10 on retrieval in SQS queues. Due to SQS behavior max messages are not ensured, but an approximate. 

One can specify visibility timeout to overrule queue defaults to ensure timeout is working with larger message batches.

TBD: warning mechanics if visibility timeouts are exceeded in processing.
