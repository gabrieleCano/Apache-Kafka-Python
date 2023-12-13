from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
print('Producer is ready!')

while True:
    print('Enter a message (\"quit\" to exit): ')
    message = input()

    if message == 'quit':
        print('Bye!')
        break

    print('Sending message: ' + message)
    producer.send('client_topic', message.encode('utf-8'))
    print('Message sent!')
