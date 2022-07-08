from typing import TypedDict, Dict
import os
import json


Header = TypedDict("Header", {"Access-Control-Allow-Origin": str})


class POST(TypedDict):
    statusCode: int
    headers: Header
    body: Dict[str, str]
