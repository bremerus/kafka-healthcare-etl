from confluent_kafka import Consumer
import json
import pandas as pd
import re
import pyodbc
from minio import Minio
from datetime import datetime
import io

consumer = Consumer({
    'bootstrap.servers' : 'localhost:9092,localhost:9093,localhost:9094',
    'group.id': 'healthcare-group',
    'auto.offset.reset': 'earliest',
    'log.connection.close': False
})

minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

DLQ_BUCKET = 'healthcare-dlq'

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=DESKTOP-H9TO6JH\SQLEXPRESS;'
    'DATABASE=HealthcareETL;'
    'Trusted_Connection=yes;'
    'TrustServerCertificate=yes'
)

cursor = conn.cursor()

GENDER_MAP = {
    'm': 'M', 'male': 'M', '1': 'M', '1.0': 'M',
    'f': 'F', 'female': 'F', '0': 'F', '0.0': 'F',
}

CURRENCY_MAP = {
    '$': 'USD', 'usd': 'USD',
    '€': 'EUR', 'eur': 'EUR',
    '£': 'GBP', 'gbp': 'GBP',
    'rs': 'INR', 'rs.': 'INR',
    '₹': 'INR', 'inr': 'INR',
}

RATES_TO_EUR = {
    'EUR': 1.0,
    'USD': 0.92,
    'GBP': 1.17,
    'INR': 0.011,
}

FOLLOW_UP_MAP = {
    '1' : True, 'y' : True, 'yes' : True,
    '0' : False, 'n' : False, 'no' : False
}

consumer.subscribe(['raw_patients'])

def is_missing(val):
    if val is None or str(val).strip() == '' or str(val).strip().lower() == 'nan':
        return True
    else:
        return False


def clean_patient(raw: dict):
    flag_dlq = False

    try:
        if is_missing(raw.get('patient_id')):
            patient_id = None
        else:
            patient_id = int(float(raw.get('patient_id')))

        if is_missing(raw.get('patient_name')):
            flag_dlq = True
            patient_name = None
        else:
            patient_name = ' '.join(str(raw.get('patient_name', '')).split())
        
        if is_missing(raw.get('age')):
            age = None
        else:
            age = int(float(raw.get('age')))

        if is_missing(raw.get('gender')):
            gender = None
        else:
            raw_gender = str(raw.get('gender')).strip().lower()
            gender = GENDER_MAP.get(raw_gender, None)

        if is_missing(raw.get('appointment_date')):
            flag_dlq = True
            appointment_date = None
        else:
            appointment_date = pd.to_datetime(raw.get('appointment_date')).strftime('%Y-%m-%d')

        if is_missing(raw.get('booking_date')):
            booking_date = None
        else:
            booking_date = pd.to_datetime(raw.get('booking_date')).strftime('%Y-%m-%d')

        if is_missing(raw.get('doctor')):
            doctor = None
        else:
            doctor = ' '.join(str(raw.get('doctor', '')).split())

        if is_missing(raw.get('department')):
            department = None
        else:
            department = str(raw.get('department')).strip()

        if is_missing(raw.get('billing_amount')):
            flag_dlq = True
            currency = None
            billing_amount_eur = None
        else:
            text_billing = str(raw.get('billing_amount')).strip()
            match = re.match(r'^([^\d.,]*)\s*([\d.,]+)\s*([^\d.,]*)$', text_billing)

            if not match:
                flag_dlq = True
                currency = None
                billing_amount_eur = None
            else:
                prefix, num_str, suffix = match.groups()
                raw_symbol = (prefix or suffix).strip().lower()
                currency = CURRENCY_MAP.get(raw_symbol)

                if currency is None:
                    flag_dlq = True
                    billing_amount_eur = None
                else:
                    amount = float(num_str.replace(',', ''))
                    billing_amount_eur = round(amount * RATES_TO_EUR[currency], 2)

        if is_missing(raw.get('follow_up_required')):
            follow_up_required = None
        else:
            raw_followup = str(raw.get('follow_up_required')).strip().lower()
            follow_up_required = FOLLOW_UP_MAP.get(raw_followup, None)

    except Exception as e:
        print(f"Parsing error on event {raw.get('event_id')}: {e}")
        return True, None

    cleaned_data = {
        'event_id': raw.get('event_id'),
        'patient_id': patient_id,
        'patient_name': patient_name,
        'age': age,
        'gender': gender,
        'appointment_date': appointment_date,
        'booking_date': booking_date,
        'doctor_name': doctor,
        'department': department,
        'currency': currency,
        'billing_amount': billing_amount_eur,
        'follow_up_required': follow_up_required
    }

    
    return flag_dlq, cleaned_data

def send_json(patient: dict):
    query = """
        if not exists (select 1 from patient_info where event_id = ?)
        begin
            insert into patient_info(
                event_id,
                patient_id,
                patient_name,
                age,
                gender,
                appointment_date,
                booking_date,
                doctor_name,
                department,
                currency_init,
                billing_amount_in_eur,
                follow_up_required
            )
            values(?,?,?,?,?,?,?,?,?,?,?,?)
        end
    """

    cursor.execute(query, (
        patient['event_id'],
        patient['event_id'],
        patient['patient_id'],
        patient['patient_name'],
        patient['age'],
        patient['gender'],
        patient['appointment_date'],
        patient['booking_date'],
        patient['doctor_name'],
        patient['department'],
        patient['currency'],
        patient['billing_amount'],
        patient['follow_up_required'],
    ))
    conn.commit()

def send_to_minio(patient: dict):
    event_id = patient.get('event_id', 'unknown')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f'dlq_event_{event_id}_{timestamp}.json'

    json_bytes = json.dumps(patient, indent=2).encode('utf-8')
    data_stream = io.BytesIO(json_bytes)

    minio_client.put_object(
        bucket_name=DLQ_BUCKET,
        object_name=file_name,
        data=data_stream,
        length=len(json_bytes),
        content_type="application/json"
    )

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue
        if message.error():
            print(f'Consumer Error: {message.error()}')
            continue

        patient = json.loads(message.value().decode('utf-8'))

        print(f"Received partition: {message.partition()} | offset: {message.offset()} | Name: {patient.get('patient_name')}")

        flag_dlq, cleaned_data = clean_patient(patient)

        if flag_dlq:
            send_to_minio(patient)
            print(f"[DLQ] Event {patient.get('event_id')} -> {patient}")
        else:
            send_json(cleaned_data)
            print(f"[CLEAN] Event {cleaned_data['event_id']} -> {cleaned_data}")
except KeyboardInterrupt:
    print('Stopping consumer')
finally:
    consumer.close()
    cursor.close()
    conn.close()