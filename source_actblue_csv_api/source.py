#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import base64
import time
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, Optional, Tuple

import requests
from numpy import nan, float64
from pandas import read_csv, BooleanDtype
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.utils.sentry import AirbyteSentry

logger = AirbyteLogger()

class HttpBasicAuthenticator(TokenAuthenticator):
    """
    Adds a Basic Authentication Header to HTTP Request
    """
    def __init__(self, auth: Tuple[str, str], auth_method: str = "Basic", **kwargs):
        auth_string = f"{auth[0]}:{auth[1]}".encode("utf8")
        b64_encoded = base64.b64encode(auth_string).decode("utf8")
        super().__init__(token=b64_encoded, auth_method=auth_method, **kwargs)


# Basic full refresh stream
class ActblueCsvApiStream(HttpStream):
    """
    Full Refresh Stream for ActBlue CSV API.
    """

    url_base = "https://secure.actblue.com/api/v1/"
    http_method = "POST"
    POLLING_INITIAL_DELAY = 1
    POLLING_BACKOFF = 2  # Will be used to increment delay since CSV creation time may vary
    POLLING_MAX_TRIES = 10  # At this point we have been waiting for around 17 mins. We should check with ActBlue how long this should take max.
    DEFAULT_TIMESTAMP_FORMAT = "%Y-%m-%d"

    @property
    def csv_type(self) -> str:
        """
        :return: The CSV type that should be requested from ActBlue API
        """

    def __init__(self, date_range_start: str, **kwargs):
        super().__init__(**kwargs)
        self.date_range_start = datetime.strptime(date_range_start, self.DEFAULT_TIMESTAMP_FORMAT)
        self.date_range_end = datetime.utcnow()
        self.http_method = "POST"
    
    def get_date_ranges(
        self,
        date_range_start: datetime,
        date_range_end: datetime,
        interval: timedelta
    ) -> List[Tuple[datetime]]:
        """
        The ActBlue CSV API is limited to only getting 6 months worth of data.
        This function will be used to return a set of date ranges split by 24 week intervals
        to meet the full date_range specified.

        Parameters:
        :date_range_start: A datetime signifying the start of the total date range
        :date_range_end: A datetime signifying the end of the total date range
        :interval: The timedelta that each sub date range
        """
        date_ranges = []
        sub_date_range_start = date_range_start

        while sub_date_range_start < date_range_end:
            sub_date_range_end = sub_date_range_start + interval
            if sub_date_range_end >= date_range_end:
                sub_date_range_end = date_range_end
            date_ranges.append((sub_date_range_start, sub_date_range_end))
            sub_date_range_start = sub_date_range_end

        return date_ranges

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "csvs"
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None
    
    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """
        The ActBlue CSV API has a rate limit of 10 requests per minute. Since we
        have to split requests into <6 month intervals this can end up hitting that 
        limit pretty easily. Rather than using exponential backoff, lets just set backoff
        time to 1 minute (Automatically retried 5 times already too)

        :param response:
        :return how long to backoff in seconds. The return value may be a floating point 
            number for subsecond precision. Returning None defers backoff
            to the default backoff behavior (e.g using an exponential algorithm).
        """
        return 60

    def request_body_json(
        self,
        date_range_start: str,
        date_range_end: str,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        """
        Returns the request body for each date range CSV POST request.
        """
        request_body = {
            "csv_type": self.csv_type,
            "date_range_start": date_range_start,
            "date_range_end": date_range_end
        }
        return request_body


    def get_download_url(self, csv_id: str) -> str:
        """
        GET request to retrieve download_url for generated CSV.
        `Args:`
            csv_id: str
                Unique identifier of the CSV you requested.
        `Returns:`
            While CSV is being generated, 'None' is returned. When CSV is ready, the method returns
            the download_url.
        """
        delay = self.POLLING_INITIAL_DELAY

        for poll_try in range(self.POLLING_MAX_TRIES):
            try:
                response = requests.get(
                    url=f"{self.url_base}csvs/{csv_id}",
                    headers=self.authenticator.get_auth_header()
                )
                response.raise_for_status()

                if response.json()["status"] == "complete":
                    return response.json()["download_url"]

                time.sleep(delay)
                logger.info(f"Polling {self.url_base}csvs/{csv_id}... sleeping for {delay} seconds")
                delay *= self.POLLING_BACKOFF
            except requests.HTTPError as http_error:
                logger.info(f"CSV Polling try #{poll_try} failed with: {http_error}")
        raise Exception(f"Polling for {self.url_base}csvs/{csv_id} has timed out")

    def get_stream_schema(self):
        """
        Converts the JSON Schema for the stream into schema that can be used for
        loading Pandas dataframe.
        """
        json_to_pandas_schema_match = {
            "string": object,
            "number": float64,
            "integer": 'Int64',
            "bool": BooleanDtype
        }
        pandas_schema = {}
        json_schema = self.get_json_schema()['properties']
        for field_name, field_types in json_schema.items():
            field_type = field_types['type'][1]
            pandas_schema[field_name] = (
                json_to_pandas_schema_match.get(field_type, object)
            )

        return pandas_schema

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Gets the CSV Request ID, Polls until CSV is ready, and finally reads CSV records
        and converts to JSON records which get yielded.
        """
        csv_request_id = response.json()["id"]
        csv_url = self.get_download_url(csv_request_id)
        columns_to_drop = [
            "Recipient Gov ID",
            "Donor US Passport Number",
            "Card Last4",
            "Card Expiration",
            "Card Type",
            "Card AVS"
        ]

        schema = self.get_stream_schema()
        
        for df in read_csv(csv_url, header=0, chunksize=10000, dtype=schema):
            yield from (
                df.replace({nan:None}).drop(columns=columns_to_drop, errors="ignore").to_dict(orient="records")
            )


    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        next_page_token = None
        with AirbyteSentry.start_transaction("read_records", self.name), AirbyteSentry.start_transaction_span("read_records"):
            for date_range in self.get_date_ranges(self.date_range_start, self.date_range_end, timedelta(weeks=24)):
                # Generate and Parse all CSVs for given Date Ranges
                request_headers = self.request_headers(
                    stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token
                )
                request = self._create_prepared_request(
                    path=self.path(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
                    headers=dict(request_headers, **self.authenticator.get_auth_header()),
                    params=self.request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
                    json=self.request_body_json(
                        date_range_start=datetime.strftime(date_range[0], self.DEFAULT_TIMESTAMP_FORMAT),
                        date_range_end=datetime.strftime(date_range[1], self.DEFAULT_TIMESTAMP_FORMAT),
                        stream_state=stream_state,
                        stream_slice=stream_slice,
                        next_page_token=next_page_token
                    ),
                )
                request_kwargs = self.request_kwargs(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)

                response = self._send_request(request, request_kwargs)

                yield from self.parse_response(response, stream_state=stream_state, stream_slice=stream_slice)

                # Always return an empty generator just in case no records were ever yielded
                yield from []


class PaidContributionsStream(ActblueCsvApiStream):
    """
    Stream for the Paid Contributions API Type
    """
    primary_key = "lineitem_id"
    csv_type = "paid_contributions"


class RefundedContributionsStream(ActblueCsvApiStream):
    """
    Stream for the Refunded Contributions API Type
    """
    primary_key = "lineitem_id"
    csv_type = "refunded_contributions"


class ManagedFormContributionsStream(ActblueCsvApiStream):
    """
    Stream for the Managed Form Contributions API Type
    """
    primary_key = "lineitem_id"
    csv_type = "managed_form_contributions"


class CancelledRecurringContributionsStream(ActblueCsvApiStream):
    """
    Stream for the Managed Form Contributions API Type
    """
    primary_key = "Receipt ID"
    csv_type = "cancelled_recurring_contributions"

# Source
class SourceActblueCsvApi(AbstractSource):
    """
    Airbyte Source Connector that pulls data from the ActBlue CSV API.
    https://secure.actblue.com/docs/csv_api
    """
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Make a request to CSV API with invalid date params to ensure we can reach
        and autheticate successfully to the API.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to
                                  the API successfully, (False, error) otherwise.
        """
        # Set endpoint params
        uuid = config["actblue_client_uuid"]
        secret = config["actblue_client_secret"]
        end_date = datetime.utcnow()
        start_date = end_date + timedelta(days=1)
        csv_type = "paid_contributions"

        connection_endpoint = "https://secure.actblue.com/api/v1/csvs"

        # You can hit the api without generating any data by providing a start date that's
        # after the end date if that's helpful. This will return a 422 Unprocessable Error
        # but that means it was able to successfully authenticate with given credentials
        # but request body was wrong.
        response = requests.post(
            url=connection_endpoint,
            auth=(uuid, secret),
            data={
                "csv_type": csv_type,
                "date_range_start": start_date.strftime("%Y-%m-%d"),
                "date_range_end": end_date.strftime("%Y-%m-%d")
            }
        )

        # Check API response
        try:
            response.raise_for_status()
        except requests.HTTPError as http_error:
            if response.status_code == 422:
                return (True, None)
            return (False, http_error)
        except Exception as error:
            return (False, error)

     
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Returns a List of Streams for the ActBlue CSV API Source

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        auth = HttpBasicAuthenticator(
            (
                config["actblue_client_uuid"],
                config["actblue_client_secret"],
            ),
        )

        return [
            PaidContributionsStream(
                authenticator=auth,
                date_range_start=config["date_range_start"]
            ),
            RefundedContributionsStream(
                authenticator=auth,
                date_range_start=config["date_range_start"]
            ),
            ManagedFormContributionsStream(
                authenticator=auth,
                date_range_start=config["date_range_start"]
            ),
            CancelledRecurringContributionsStream(
                authenticator=auth,
                date_range_start=config["date_range_start"]
            )
        ]
