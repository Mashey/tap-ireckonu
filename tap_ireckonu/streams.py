import singer
from singer import logger
from datetime import datetime, timedelta

LOGGER = singer.get_logger()

def end_date_parse(start_date: str) -> str:
    return datetime.strftime(
        (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)), "%Y-%m-%d"
    )


class Stream:
    tap_stream_id = None
    key_properties = []
    replication_method = ""
    valid_replication_keys = []
    replication_key = ""
    object_type = ""
    today = datetime.strftime(datetime.today(), "%Y-%m-%d")

    def __init__(self, client, state):
        self.client = client
        self.state = state

    def sync_records(self, *args, **kwargs):
        raise NotImplementedError("Sync of child class not implemented")


class CatalogStream(Stream):
    replication_method = "INCREMENTAL"


class FullTableStream(Stream):
    replication_method = "FULL_TABLE"


class BulkCompany(CatalogStream):
    tap_stream_id = "company"
    key_properties = ["id"]
    object_type = "company"
    page = 0
    page_size = 100

    def sync(self, start_date, hotel_codes):
        ## This is where to setup iteration over each end point
        self.client.fetch_access_token()
        start_date = singer.get_bookmark(
            self.state, self.tap_stream_id, self.replication_key, start_date
        )
        end_date = end_date_parse(start_date)

        while start_date != self.today:
            response_length = self.page_size
            # LOGGER.info(f'Syncing for Date: {start_date}')
            while response_length >= self.page_size:
                response = self.client.fetch_bulk_company(
                    start_date=start_date,
                    end_date=end_date,
                    page=self.page,
                    page_size=self.page_size,
                )

                response_length = len(response["Data"])
                companies = response["Data"]
                for company in companies:
                    yield company

            start_date = end_date
            end_date = end_date_parse(end_date)


class BulkPerson(CatalogStream):
    tap_stream_id = "person"
    key_properties = ["id"]
    object_type = "person"
    page = 0
    page_size = 100

    def sync(self, start_date, hotel_codes):
        ## This is where to setup iteration over each end point
        self.client.fetch_access_token()
        start_date = singer.get_bookmark(
            self.state, self.tap_stream_id, self.replication_key, start_date
        )
        end_date = end_date_parse(start_date)

        while start_date != self.today:
            response_length = self.page_size
            # LOGGER.info(f'Syncing for Date: {start_date}')
            while response_length >= self.page_size:
                response = self.client.fetch_bulk_person(
                    start_date=start_date,
                    end_date=end_date,
                    page=self.page,
                    page_size=self.page_size,
                )

                response_length = len(response["Data"])
                persons = response["Data"]
                for person in persons:
                    yield person

            start_date = end_date
            end_date = end_date_parse(end_date)



class BulkFolio(CatalogStream):
    tap_stream_id = "folio"
    key_properties = ["id"]
    object_type = "folio"
    page = 0
    page_size = 100

    def sync(self, start_date, hotel_codes):
        ## This is where to setup iteration over each end point
        self.client.fetch_access_token()
        start_date = singer.get_bookmark(
            self.state, self.tap_stream_id, self.replication_key, start_date
        )
        end_date = end_date_parse(start_date)

        while start_date != self.today:
            response_length = self.page_size
            # LOGGER.info(f'Syncing for Date: {start_date}')
            while response_length >= self.page_size:
                response = self.client.fetch_bulk_folio(
                    start_date=start_date,
                    end_date=end_date,
                    page=self.page,
                    page_size=self.page_size,
                )

                response_length = len(response["Data"])
                folios = response["Data"]
                for folio in folios:
                    yield folio

            start_date = end_date
            end_date = end_date_parse(end_date)



class BulkReservation(CatalogStream):
    tap_stream_id = "reservation"
    key_properties = ["id"]
    object_type = "reservation"
    page = 0
    page_size = 100

    def sync(self, start_date, hotel_codes):
        ## This is where to setup iteration over each end point
        self.client.fetch_access_token()

        for hotel_code in hotel_codes:
            LOGGER.info(f"Starting Reservations for Hotel Code: {hotel_code}")
            start_date = singer.get_bookmark(
                self.state, self.tap_stream_id, self.replication_key, start_date
            )
            end_date = end_date_parse(start_date)

            while start_date != self.today:
                response_length = self.page_size
                # LOGGER.info(f'Syncing for Date: {start_date}')
                while response_length >= self.page_size:
                    response = self.client.fetch_bulk_reservation(
                        hotel_code=hotel_code,
                        start_date=start_date,
                        end_date=end_date,
                        page=self.page,
                        page_size=self.page_size,
                    )

                    response_length = len(response["Data"])
                    reservations = response["Data"]
                    for reservation in reservations:
                        yield reservation

                start_date = end_date
                end_date = end_date_parse(end_date)


class BulkHouseAccount(CatalogStream):
    tap_stream_id = "house_account"
    key_properties = ["id"]
    object_type = "house_account"
    page = 0
    page_size = 100

    def sync(self, start_date, hotel_codes):
        ## This is where to setup iteration over each end point
        self.client.fetch_access_token()

        for hotel_code in hotel_codes:
            LOGGER.info(f"Starting House Accounts for Hotel Code: {hotel_code}")
            start_date = singer.get_bookmark(
                self.state, self.tap_stream_id, self.replication_key, start_date
            )
            end_date = end_date_parse(start_date)

            while start_date != self.today:
                response_length = self.page_size
                # LOGGER.info(f'Syncing for Date: {start_date}')
                while response_length >= self.page_size:
                    response = self.client.fetch_bulk_house_account(
                        hotel_code=hotel_code,
                        start_date=start_date,
                        end_date=end_date,
                        page=self.page,
                        page_size=self.page_size,
                    )

                    response_length = len(response["Data"])
                    house_accounts = response["Data"]
                    for house_account in house_accounts:
                        yield house_account

                start_date = end_date
                end_date = end_date_parse(end_date)


STREAMS = {
    "company": BulkCompany,
    "person": BulkPerson,
    "folio": BulkFolio,
    "reservation": BulkReservation,
    "house_account": BulkHouseAccount,
}
