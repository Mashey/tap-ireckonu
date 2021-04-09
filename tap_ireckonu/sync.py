import singer
from singer import Transformer, metadata
import time
from datetime import datetime, timedelta

# The client name needs to be filled in here
from tap_ireckonu.client import IreckonuClient
from tap_ireckonu.streams import STREAMS

LOGGER = singer.get_logger()


def sync(config, state, catalog):
    # Any client required PARAMETERS to hit the endpoint
    client = IreckonuClient(config)
    total_records = []
    stream_rps = []

    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            stream_start = time.perf_counter()
            record_count = 0
            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client, state)
            replication_key = stream_obj.replication_key
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info('Staring sync for stream: %s', tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key
            )

            client = IreckonuClient(config)
            for record in stream_obj.sync():
                transformed_record = transformer.transform(
                    record, stream_schema, stream_metadata)

                singer.write_record(
                    tap_stream_id,
                    transformed_record,
                )
                record_count += 1

            # If there is a Bookmark or state based key to store
            state = singer.write_bookmark(
                state, tap_stream_id, 'Last Run Date', datetime.strftime(datetime.today(), "%Y-%m-%d"))
            stream_stop = time.perf_counter()

            total_records.append(record_count)
            info, rps = metrics(stream_start, stream_stop, record_count)
            stream_rps.append(rps)
            LOGGER.info(f"{info}")
            singer.write_bookmark(state, tap_stream_id, "metrics", info)

            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    overall_rps = overall_metrics(total_records, stream_rps)
    LOGGER.info(
        f"Total Records: {sum(total_records)} / Overall RPS: {overall_rps:0.6}"
    )
    singer.write_bookmark(state, "Overall", "metrics",
                          f"Records: {sum(total_records)} / RPS: {overall_rps:0.6}")
    singer.write_state(state)


def metrics(start: float, end: float, records: int):
    elapsed_time = end - start
    rps = records / elapsed_time
    info = f"Stream runtime: {elapsed_time:0.6} seconds / Records: {records} / RPS: {rps:0.6}"
    return info, rps


def overall_metrics(records: list, rps_list: list) -> float:
    stream_count = len(records)
    total_rps = sum(rps_list)
    return total_rps / stream_count
