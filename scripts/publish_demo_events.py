import json, time

def emit(rec):
    # Replace this print with a real producer if available (Kafka, REST, etc.)
    print(json.dumps(rec), flush=True)

now = int(time.time())
events = [
    {"event":"order_created","order_id":"A100","amount":49.9,"ts":now},
    {"event":"order_created","orderId":"A101","amt":59.0,"ts":now+1},  # schema drift
    {"event":"order_created","order_id":"A099","amount":39.0,"ts":now-3600},  # late arrival
]

for e in events:
    emit(e)
    time.sleep(0.3)