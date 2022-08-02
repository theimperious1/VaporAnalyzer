import json
import time
import traceback

import requests
from DataManager import *
from threading import Thread
import logging

API_URL = 'https://io.dexscreener.com/u/trading-history/recent/avalanche/0x4cd20F3e2894Ed1A0F4668d953a98E689c647bfE'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0',
    'Origin': 'https://dexscreener.com',
    'DNT': '1',
    'Alt-Used': 'io.dexscreener.com',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Connection': 'close'
}

logger = logging.getLogger(__name__)


# https://io.dexscreener.com/u/trading-history/recent/avalanche/0x4cd20F3e2894Ed1A0F4668d953a98E689c647bfE?t=1655251832001
# anything after the timestamp above will be returned. if param is tb (time before), anything before that will be returned.
# noinspection PyBroadException
class NetworkManager:

    def __init__(self):
        self.data_manager = DataManager()
        self.timestamp = str(int(time.time())) + '000'

    def fetch_data_once(self):
        req = requests.get(f'{API_URL}?tb={self.timestamp}', headers=headers)
        return json.loads(req.text)

    def fetch_data(self):
        iterator = 1
        count = 0
        while 1:
            try:
                data = self.fetch_data_once()
                if not self.integrate_data(data):
                    return False

                if iterator % 100:
                    logger.info(f'Loop {count} complete!')
                    iterator = 0
                iterator += 1
                count += 1
            except SystemExit:
                break
            except:
                traceback.print_exc()
                exit()

        return True

    def integrate_data(self, trading_data):
        trades = trading_data['tradingHistory']
        end = len(trades)
        iterator = 0
        for trade in trades:
            if not self.data_manager.insert(trade):
                return False
            iterator += 1
            if iterator == end:
                self.timestamp = trade['blockTimestamp']

        return True

    def get_last_timestamp(self):
        return self.timestamp

    def get_data_manager(self):
        return self.data_manager
