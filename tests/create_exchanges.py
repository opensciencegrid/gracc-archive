import pika

credentials = pika.PlainCredentials("guest", "guest")
parameters = pika.ConnectionParameters("localhost",
                                                5672, "/", credentials)
conn = pika.adapters.blocking_connection.BlockingConnection(parameters)

channel = conn.channel()

channel.exchange_declare(exchange="gracc.osg.raw", exchange_type='fanout', durable=True, auto_delete=False)


