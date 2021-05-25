import singer
from singer import Transformer, metadata
import time
from datetime import datetime, timedelta

# The client name needs to be filled in here
from tap_ireckonu.client import IreckonuClient
from tap_ireckonu.streams import STREAMS
from tap_ireckonu.utils import AuditLogs, SlackMessenger


LOGGER = singer.get_logger()


def sync(config, state, catalog):
    # Any client required PARAMETERS to hit the endpoint
    client = IreckonuClient(config)

    run_id = int(time.time())
    pipeline_start = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
    pipeline_start_time = time.perf_counter()
    stream_comments = []
    total_records = 0

    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            batch_start = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
            start_time = time.perf_counter()
            record_count = 0

            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client, state)
            replication_key = stream_obj.replication_key
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info("Staring sync for stream: %s", tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key,
            )

            try:
                for record in stream_obj.sync(config['start_date'], config['hotel_codes']):
                    transformed_record = transformer.transform(
                        record, stream_schema, stream_metadata
                    )

                    singer.write_record(
                        tap_stream_id,
                        transformed_record,
                    )
                    record_count += 1
                    total_records += 1

                # If there is a Bookmark or state based key to store
                state = singer.write_bookmark(
                    state,
                    tap_stream_id,
                    "Last Run Date",
                    datetime.strftime(datetime.today(), "%Y-%m-%d"),
                )
                singer.write_state(state)

                batch_stop = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
                AuditLogs.write_audit_log(
                    run_id=run_id,
                    stream_name=tap_stream_id,
                    batch_start=batch_start,
                    batch_end=batch_stop,
                    records_synced=record_count,
                    run_time=(time.perf_counter() - start_time),
                )

            except Exception as e:
                stream_comments.append(f"{tap_stream_id.upper}: {e}")
                batch_stop = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
                AuditLogs.write_audit_log(
                    run_id=run_id,
                    stream_name=tap_stream_id,
                    batch_start=batch_start,
                    batch_end=batch_stop,
                    records_synced=record_count,
                    run_time=(time.perf_counter() - start_time),
                    comments=e,
                )

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)

    # Comment out for local runs
    if config["slack_notifications"] == True:
        SlackMessenger.send_message(
            run_id=run_id,
            start_time=pipeline_start,
            run_time=(time.perf_counter() - pipeline_start_time),
            record_count=total_records,
            comments='\n'.join(stream_comments),
        )
