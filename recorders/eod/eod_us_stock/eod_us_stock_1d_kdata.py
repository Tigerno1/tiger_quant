import os
from core.requester import RequestGet, JsonFormat, CSVFormat

from conf import EOD_DAILY

class EodUSStock1dKdata:

    def __init__(self):
        pass 

    def Get(self):
        resp = RequestGet(JsonFormat()).fetch(EOD_DAILY)
        print(resp)


if __name__ == "__main__":
    EodUSStock1dKdata().Get()
