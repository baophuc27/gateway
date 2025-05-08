from pydantic import BaseModel

class MessageRequest(BaseModel):
    topic: str
    message: str
    code: str
