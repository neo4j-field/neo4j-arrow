import pyarrow as pa, pyarrow.parquet
import neo4j_arrow as na
from google.cloud import bigquery

import os, sys, threading, queue
from time import sleep, time

### Config
HOST = os.environ.get('NEO4J_ARROW_HOST', 'localhost')
PORT = int(os.environ.get('NEO4J_ARROW_PORT', '9999'))
USERNAME = os.environ.get('NEO4J_USERNAME', 'neo4j')
PASSWORD = os.environ.get('NEO4J_PASSWORD', 'password')
GRAPH = os.environ.get('NEO4J_GRAPH', 'random')
PROPERTIES = [
        p for p in os.environ.get('NEO4J_PROPERTIES', '').split(',') 
        if len(p) > 0]
TLS = len(os.environ.get('NEO4J_ARROW_TLS', '')) > 0
TLS_VERIFY = len(os.environ.get('NEO4J_ARROW_TLS_NO_VERIFY', '')) < 1
DATASET = os.environ.get('BQ_DATASET', 'neo4j_arrow')
TABLE = os.environ.get('BQ_TABLE', 'nodes')
DELETE = len(os.environ.get('BQ_DELETE_FIRST', '')) > 0

### Globals
bq_client = bigquery.Client()
q = queue.Queue(maxsize=32)
done_feeding = threading.Event()

### Various Load Options
parquet_options = bigquery.format_options.ParquetOptions()
parquet_options.enable_list_inference = True
bigquery.dataset = DATASET
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.PARQUET
job_config.parquet_options = parquet_options


def upload_complete(load_job):
    # print(f'bq upload complete ({load_job.job_id})')
    q.task_done()

def write_to_bigquery(writer_id, table, job_config):
    """Convert a PyArrow Table to a Parquet file and load into BigQuery"""
    writer = pa.BufferOutputStream()
    pa.parquet.write_table(table, writer, use_compliant_nested_type=True)
    pq_reader = pa.BufferReader(writer.getvalue())

    load_job = bq_client.load_table_from_file(
            pq_reader, f'{DATASET}.{TABLE}', job_config=job_config)
    load_job.add_done_callback(upload_complete)
    return load_job

def bq_writer(writer_id):
    """Primary logice for a BigQuery writer thread"""
    global done_feeding, job_config
    jobs = []
    print(f"w({writer_id}): writer starting")

    while True:
        try:
            batch = q.get(timeout=5)
            if len(batch) < 1:
                break
            table = pa.Table.from_batches(batch, batch[0].schema)
            load_job = write_to_bigquery(writer_id, table, job_config)
            jobs.append(load_job)
        except queue.Empty:
            # use this as a chance to cleanup our jobs
            _jobs = []
            for j in jobs:
                if j.running():
                    _jobs.append(j)
                elif j.error_result:
                    # consider this fatal for now...might have hit a rate-limit!
                    print(f"w({writer_id}): job {j} had an error {j.error_result}!!!")
                    sys.exit(1)
            if len(_jobs) > 0:
                print(f"w({writer_id}): waiting on {len(jobs)} bq load jobs")
            elif done_feeding.is_set():
                break
    print(f"w({writer_id}): finished")


def stream_records(reader):
    """Consume a neo4j-arrow GDS stream and populate a work queue"""
    print('Start arrow table processing')

    cnt, rows, nbytes = 0, 0, 0
    batch = []
    start = time()
    for chunk, metadata, in reader:
        cnt = cnt + chunk.num_rows
        rows = rows + chunk.num_rows
        nbytes = nbytes + chunk.nbytes
        batch.append(chunk)
        if rows >= 100_000:
            q.put(batch)
            nbytes = (nbytes >> 20)
            print(f"stream row @ {cnt:,}, batch size: {rows:,} rows, {nbytes:,} MiB")
            batch = []
            rows, nbytes = 0, 0
    if len(batch) > 0:
        # add any remaining data
        q.put(batch)

    # signal we're done consuming the source feed and wait for work to complete
    done_feeding.set()
    q.join()
    
    finish = time()
    print(f"Done! Time Delta: {round(finish - start, 1):,}s")
    print(f"Count: {cnt:,} rows, Rate: {round(cnt / (finish - start)):,} rows/s")


if __name__ == "__main__":
    print("Creating neo4j-arrow client")
    client = na.Neo4jArrow(USERNAME, PASSWORD, (HOST, PORT),
            tls=TLS, verifyTls=TLS_VERIFY)

    print("Submitting read job for graph '{GRAPH}'")
    ticket = client.gds_nodes(GRAPH, properties=PROPERTIES)
 
    print("Starting worker threads")
    threads = []
    for i in range(0, 12):
        t = threading.Thread(target=bq_writer, daemon=True, args=[i])
        threads.append(t)
        t.start()

    print(f"Streaming nodes from {GRAPH} with properties {PROPERTIES}")
    client.wait_for_job(ticket, timeout=180)
    reader = client.stream(ticket)
    stream_records(reader)

    # try to nicely wait for threads to finish just in case
    for t in threads:
        t.join(timeout=60)

