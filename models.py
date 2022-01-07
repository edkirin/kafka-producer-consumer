from pydantic import BaseModel


class User(BaseModel):
    uuid: str
    first_name: str
    last_name: str
    email: str
