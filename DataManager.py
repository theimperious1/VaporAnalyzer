from enum import Enum
import sqlite3
from collections import Counter


class SortTypes(Enum):
    WITHIN_RANGE = 0
    HIGHEST_USD_PRICE = 1
    LOWEST_USD_PRICE = 2
    HIGHEST_USD_VOLUME = 3
    LOWEST_USD_VOLUME = 4
    HIGHEST_VPND = 5
    LOWEST_VPND = 6
    HIGHEST_AVAX = 7
    LOWEST_AVAX = 8
    MOST_COMMON_PRICE = 9
    MOST_COMMON_PRICES = 10


COLUMNS = {
    'blockNumber': 0,
    'blockTimestamp': 1,
    'txnHash': 2,
    'logIndex': 3,
    'type': 4,
    'priceUsd': 5,
    'volumeUsd': 6,
    'amountVpnd': 7,
    'amountAvax': 8
}


def format_number(num):
    return float(num.replace(',', ''))


class DataManager:

    def __init__(self):
        self.db = sqlite3.connect('data.db', check_same_thread=False)
        self.transactions = {'sell': [], 'buy': []}
        self.all_transactions = {}
        with self.db:
            cur = self.db.cursor()
            cur.execute('SELECT * FROM transactions')
            for row in cur.fetchall():
                self.transactions[row[4]].append(row)
                self.all_transactions[row[2]] = row

    def insert(self, transaction):
        if transaction['txnHash'] in self.all_transactions:
            return False

        transaction = (transaction['blockNumber'], transaction['blockTimestamp'], transaction['txnHash'],
                       transaction['logIndex'], transaction['type'], format_number(transaction['priceUsd']),
                       format_number(transaction['volumeUsd']), format_number(transaction['amount0']),
                       format_number(transaction['amount1']), 0)

        with self.db:
            cur = self.db.cursor()
            cur.execute('INSERT INTO transactions VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING',
                        transaction)
            self.transactions[transaction[4]].append(transaction)
            self.db.commit()

        return True

    def sort(self, column, sort_type, sort_value_1=None, sort_value_2=None, round_it=False, round_to=-3):
        def most_common():
            sell_data = []
            buy_data = []
            for key in self.transactions:
                for trade in self.transactions[key]:
                    if trade[4] == 'sell' and round_it:
                        sell_data.append(round(trade[COLUMNS[column]], round_to))
                    elif trade[4] == 'sell' and not round_it:
                        sell_data.append(int(trade[COLUMNS[column]]))
                    elif trade[4] == 'buy' and round_it:
                        buy_data.append(round(trade[COLUMNS[column]], round_to))
                    elif trade[4] == 'buy' and not round_it:
                        buy_data.append(int(trade[COLUMNS[column]]))

            sell_data = Counter(sell_data)
            buy_data = Counter(buy_data)
            return sell_data.most_common(500), buy_data.most_common(500)

        if sort_type == SortTypes.MOST_COMMON_PRICES:
            return most_common()

        with self.db:
            cur = self.db.cursor()
            if sort_type == SortTypes.WITHIN_RANGE:
                cur.execute(f'SELECT * FROM transactions WHERE {column} BETWEEN {sort_value_1} AND {sort_value_2}')
            elif sort_type == SortTypes.HIGHEST_USD_PRICE:
                cur.execute(f'SELECT * FROM transactions ORDER BY priceUsd DESC')
            elif sort_type == SortTypes.LOWEST_USD_PRICE:
                cur.execute(f'SELECT * FROM transactions ORDER BY priceUsd ASC')
            elif sort_type == SortTypes.HIGHEST_USD_VOLUME:
                cur.execute(f'SELECT * FROM transactions ORDER BY volumeUsd DESC')
            elif sort_type == SortTypes.LOWEST_USD_VOLUME:
                cur.execute(f'SELECT * FROM transactions ORDER BY volumeUsd ASC')
            elif sort_type == SortTypes.HIGHEST_VPND:
                cur.execute(f'SELECT * FROM transactions ORDER BY amountVpnd DESC')
            elif sort_type == SortTypes.LOWEST_VPND:
                cur.execute(f'SELECT * FROM transactions ORDER BY amountVpnd DESC')
            elif sort_type == SortTypes.HIGHEST_AVAX:
                cur.execute(f'SELECT * FROM transactions ORDER BY amountAvax DESC')
            elif sort_type == SortTypes.LOWEST_AVAX:
                cur.execute(f'SELECT * FROM transactions ORDER BY amountAvax DESC')
            elif sort_type == SortTypes.MOST_COMMON_PRICE:
                cur.execute(
                    f'SELECT {column} FROM transactions GROUP BY {column} HAVING COUNT(*) = (SELECT MAX(Cnt) FROM (SELECT COUNT(*) as Cnt FROM transactions GROUP BY {column} ) tmp )')

            return cur.fetchall()

    def get_transactions(self):
        return self.transactions

    def get_cursor(self, callback):
        with self.db:
            cur = self.db.cursor()
            callback(cur)
