import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage

NAMESPACE_CONNECTION_STR = "Endpoint=sb://pub-sub-demo-1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=poAgYnAgdh7QLfdl1tLLf+JjRY9V5JROt+ASbKyy53I=" //secret!!!
TOPIC_NAME = "topic-demo-1"

async def send_single_message(sender):
    message = ServiceBusMessage("Single Message")
    await sender.send_messages(message)
    print("Sent a single message")

async def send_a_list_of_messages(sender):
    messages = [ServiceBusMessage("Message in list") for _ in range(5)]
    await sender.send_messages(messages)
    print("Sent a list of 5 messages")

async def send_batch_message(sender):
    async with sender:
        batch_message = await sender.create_message_batch()
        for _ in range(10):
            try:
                batch_message.add_message(ServiceBusMessage("Message inside a ServiceBusMessageBatch"))
            except ValueError:
                break
        await sender.send_messages(batch_message)
    print("Sent a batch of 10 messages")

async def run():
    async with ServiceBusClient.from_connection_string(
        conn_str=NAMESPACE_CONNECTION_STR,
        logging_enable=True) as servicebus_client:
        sender = servicebus_client.get_topic_sender(topic_name=TOPIC_NAME)
        async with sender:
            await send_single_message(sender)
            await send_a_list_of_messages(sender)
            await send_batch_message(sender)

asyncio.run(run())
print("Done sending messages")
print("-----------------------")
