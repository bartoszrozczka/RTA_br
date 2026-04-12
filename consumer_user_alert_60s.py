from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_transactions = defaultdict(deque)


for message in consumer:
    event = message.value
    user_id = event['user_id']

    transaction_time = datetime.fromisoformat(event['timestamp'])
    transaction_queue = user_transactions[user_id]
    transaction_queue.append(transaction_time)
    
    time_window = transaction_time - timedelta(seconds=60)
    while transaction_queue[0] < time_window:
        transaction_queue.popleft()

    if len(transaction_queue) > 3:
        print(f"ALERT: user {user_id} wykonał {len(transaction_queue)} transakcji w oknie.\n")
