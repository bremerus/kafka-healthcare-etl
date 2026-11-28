from confluent_kafka import Producer
import pandas as pd

import time

producer = Producer({
    'bootstrap.servers' : 'localhost:9092, localhost:9093, localhost:9094',
    'log.connection.close' : False,
    'acks' : 'all'
})

df = pd.read_csv(r'C:\Users\user\Desktop\projectakia_kafka\messy_clinic_appointments.csv')

df.insert(0, 'event_id', range(len(df)))

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery Failed: {err}')
    else:
        key = msg.key().decode('utf-8')
        print(f'Sent -> Customer: {key} | Partition: {msg.partition()} | Offset: {msg.offset()}')

patient_idx = 0

while patient_idx < len(df):
    patient_data = df.iloc[patient_idx].to_json(force_ascii=False)

    producer.produce(
        topic='raw_patients',
        key = str(patient_idx).encode('utf-8'),
        value= (patient_data).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()

    print(f'Order {patient_idx} was sent')

    patient_idx +=1

    time.sleep(2)