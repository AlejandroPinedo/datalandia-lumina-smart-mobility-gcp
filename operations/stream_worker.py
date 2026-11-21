"""Consume eventos JSONL de una fuente existente, una transacción por evento."""
import argparse
import json
import time
from pathlib import Path
from operations.event_store import EventStore


def consume(source, store, follow=False):
    with Path(source).open(encoding='utf-8') as stream:
        while True:
            position=stream.tell()
            line=stream.readline()
            if not line:
                if not follow:break
                time.sleep(.1);continue
            if follow and not line.endswith('\n'):
                stream.seek(position);time.sleep(.1);continue
            try:payload=json.loads(line)
            except ValueError:payload=line.rstrip('\n')
            yield store.process(payload)


def consume_pubsub(messages, store, acknowledge):
    """La confirmación ocurre después del commit; fallos de almacenamiento no reciben ACK."""
    outcomes=[]
    for received in messages:
        raw=received.message.data.decode('utf-8',errors='replace')
        try:payload=json.loads(raw)
        except ValueError:payload=raw
        outcome=store.process(payload)
        acknowledge(received.ack_id)
        outcomes.append(outcome)
    return outcomes


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',required=True);parser.add_argument('--db',required=True)
    parser.add_argument('--follow',action='store_true');args=parser.parse_args()
    store=EventStore(args.db)
    try:
        for result in consume(args.input,store,args.follow):print(result,flush=True)
    except KeyboardInterrupt:pass
    finally:store.close()
