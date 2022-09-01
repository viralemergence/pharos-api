from typing import TypedDict, Dict

Header = TypedDict("Header", {"Access-Control-Allow-Origin": str})


class Post(TypedDict):
    statusCode: int
    headers: Header
    body: Dict[str, str]
