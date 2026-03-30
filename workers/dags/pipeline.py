from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta
from datetime import datetime
import redis
import json
import mysql.connector

default_args = {
    'owner': 'kushik',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}


def grab_events(**context):
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    try:
        r.xgroup_create('kernovix_events', 'my_group', id='0', mkstream=True)
    except:
        pass

    messages = r.xreadgroup(
        groupname='my_group',
        consumername='worker1',
        streams={'kernovix_events': '>'},
        count=500,
        block=2000
    )

    if not messages:
        print("Nothing in Redis right now.")
        context['ti'].xcom_push(key='events', value=[])
        return

    events = []
    for stream, msgs in messages:
        for msg_id, msg_data in msgs:
            events.append({
                'id': msg_id,
                'data': json.loads(msg_data.get('data', '{}'))
            })

    print(f"Grabbed {len(events)} events from Redis")
    context['ti'].xcom_push(key='events', value=events)


def check_events(**context):
    events = context['ti'].xcom_pull(key='events', task_ids='grab_events')

    if not events:
        context['ti'].xcom_push(key='good', value=[])
        context['ti'].xcom_push(key='bad',  value=[])
        return

    good = []
    bad  = []

    for event in events:
        data = event['data']

        if 'event_type' not in data:
            bad.append({'id': event['id'], 'reason': 'missing event_type', 'data': str(data)})
            continue

        if 'source_id' not in data:
            bad.append({'id': event['id'], 'reason': 'missing source_id', 'data': str(data)})
            continue

        if 'timestamp' not in data:
            bad.append({'id': event['id'], 'reason': 'missing timestamp', 'data': str(data)})
            continue

        data['event_type'] = data['event_type'].lower().strip()
        good.append({'id': event['id'], 'data': data})

    print(f"Good: {len(good)} | Bad: {len(bad)}")
    context['ti'].xcom_push(key='good', value=good)
    context['ti'].xcom_push(key='bad',  value=bad)


def save_to_mysql(**context):
    good = context['ti'].xcom_pull(key='good', task_ids='check_events') or []
    bad  = context['ti'].xcom_pull(key='bad',  task_ids='check_events') or []

    conn = mysql.connector.connect(
        host='localhost',
        database='kernovix',
        user='root',
        password='Kushik23'   
    )
    cursor = conn.cursor()

    for e in good:
        cursor.execute("""
            INSERT INTO processed_events
                (event_type, source_id, happened_at, data, redis_id)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE saved_at = saved_at
        """, (
            e['data']['event_type'],
            e['data']['source_id'],
            e['data']['timestamp'],
            json.dumps(e['data']),
            e['id']
        ))

    for e in bad:
        cursor.execute("""
            INSERT INTO dead_letter_events (redis_id, reason, raw_data)
            VALUES (%s, %s, %s)
        """, (e['id'], e['reason'], e['data']))

    cursor.execute("""
        INSERT INTO pipeline_runs (events_good, events_bad)
        VALUES (%s, %s)
    """, (len(good), len(bad)))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Saved to MySQL — Good: {len(good)}, Bad: {len(bad)}")


def confirm_redis(**context):
    good = context['ti'].xcom_pull(key='good', task_ids='check_events') or []
    bad  = context['ti'].xcom_pull(key='bad',  task_ids='check_events') or []

    all_ids = [e['id'] for e in good + bad]
    if not all_ids:
        return

    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    r.xack('kernovix_events', 'my_group', *all_ids)
    print(f"Told Redis we finished {len(all_ids)} events")


with DAG(
    dag_id='kernovix_pipeline',
    default_args=default_args,
    schedule=timedelta(minutes=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    step1 = PythonOperator(task_id='grab_events',   python_callable=grab_events)
    step2 = PythonOperator(task_id='check_events',  python_callable=check_events)
    step3 = PythonOperator(task_id='save_to_mysql', python_callable=save_to_mysql)
    step4 = PythonOperator(task_id='confirm_redis', python_callable=confirm_redis)

    step1 >> step2 >> step3 >> step4