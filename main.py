#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_actblue_csv_api import SourceActblueCsvApi

if __name__ == "__main__":
    source = SourceActblueCsvApi()
    launch(source, sys.argv[1:])
