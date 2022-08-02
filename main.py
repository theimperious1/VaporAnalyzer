#!/usr/bin/env python
import json
import sys
import signal
import logging
import sqlite3
import time

from web3 import Web3
from NetworkManager import NetworkManager
from abi import abi

# Set up logging
logger = logging.getLogger(__name__)
formatting = "[%(asctime)s] [%(levelname)s:%(name)s] %(message)s"
# noinspection PyArgumentList
logging.basicConfig(
    format=formatting,
    level=logging.INFO,
    handlers=[logging.FileHandler('vapor.log', encoding='utf8'),
              logging.StreamHandler()])
api_manager = NetworkManager()


def signal_handler(sig, frame):
    """ Signal handler for CTRL C """
    logger.info('VaporAnalyzer was killed by Ctrl C. Making smooth exit...')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def get_column_by_num(num):
    if num == 1:
        return 'priceUsd'
    elif num == 2:
        return 'volumeUsd'
    elif num == 3:
        return 'amountVpnd'
    elif num == 4:
        return 'amountAvax'


db = sqlite3.connect('data.db', check_same_thread=False)
transactions = []
wallets = []

w3 = Web3(Web3.HTTPProvider("https://api.avax.network/ext/bc/C/rpc"))

controllerAddress = "0xD7Ce2935008Ae8ca17E90fbe2410D2DB7608058C"
storageAddress = "0xCd5E168dA3456cD2d5A8ab400f9cebdDC453720d"

# noinspection PyTypeChecker
controllerContract = w3.eth.contract(address=controllerAddress, abi=abi)
# noinspection PyTypeChecker
storageContract = w3.eth.contract(address=storageAddress, abi=abi)


def reload_data():
    with db:
        cur = db.cursor()
        cur.execute('SELECT txnHash FROM transactions WHERE scraped = 0')
        for row in cur.fetchall():
            transactions.append(row[0])
        cur.execute('SELECT address FROM wallets WHERE total_amount = -1')
        for row in cur.fetchall():
            wallets.append(row[0])

    logger.info('Successfully reloaded data!')


def update_nodes():
    i = 0
    with db:
        cur = db.cursor()
        for addr in wallets:
            wallet_address = w3.toChecksumAddress(addr)

            nodes = storageContract.functions.getAllNodes(wallet_address).call()

            node_names = []
            node_amounts = []
            total_amount = -1
            creation_time = -1
            last_claim_time = -1
            last_compound_time = -1
            for node in nodes:
                name, creation_time, last_claim_time, last_compound_time, amount, deleted = node
                if deleted:
                    continue
                node_names.append(name)
                node_amounts.append(amount / 1e18)
                total_amount += (amount / 1e18)

            cur.execute(
                'UPDATE wallets SET nodes = ?, node_amounts = ?, total_amount = ?, creation_time = ?, last_claim_time = ?, last_compound_time = ? WHERE address = ?',
                (json.dumps(node_names), json.dumps(node_amounts), total_amount, creation_time, last_claim_time,
                 last_compound_time, addr))
            db.commit()

            i += 1
            logger.info(f'{i} - {addr}')

    logger.info('Node update is complete!')
    # print(w3.eth.getTransaction('0x7a9226558e974251ad4814a851ac0c59a6ba903fe973fe6cffb7e71130bc59f2'))


def fetch_wallets():
    with db:
        cur = db.cursor()
        i = 1
        transactions.reverse()
        for tx in transactions:
            transaction = w3.eth.getTransaction(tx)
            cur.execute(
                f'INSERT INTO wallets VALUES("{transaction["from"]}", null, null, -1, -1, -1, -1, {int(time.time())}) ON CONFLICT DO NOTHING')
            cur.execute(f"UPDATE transactions SET scraped = 1 WHERE txnHash = '{tx}'")
            db.commit()
            logger.info(f'{i}: {transaction["from"]}')
            i += 1

    logger.info('Wallets have been fetched and updated from latest transaction data!')


reload_data()
while 1:
    user_input = input(
        'Commands: \n"scrape" (scrape transactions)\n"scrape_last_100"\n"fetch_wallets"\n"update_nodes"\n"reload_data"\n"sort_data" (offers sorting options and displays data)\n')
    if user_input == 'scrape':
        if not api_manager.fetch_data():
            logger.info('Scraping complete. Transaction table is up to date!')
            reload_data()
    elif user_input == 'scrape_last_100':
        api_manager.fetch_data_once()
        reload_data()
    elif user_input == 'fetch_wallets':
        fetch_wallets()
    elif user_input == 'update_nodes':
        update_nodes()
    elif user_input == 'reload_data':
        reload_data()
    elif user_input == 'exit' or user_input == 'stop':
        logger.info('Exiting...')
        sys.exit(0)
    elif user_input == 'sort_data':

        logger.info(
            '0) within_range (ex: 1000-5000)\n1) highest_usd_price\n2) lowest_usd_price\n3) highest_usd_volume\n'
            '4) lowest_usd_volume\n5) highest_vpnd\n6)lowest_vpnd\n7) highest_avax\n8) lowest_avax\n'
            '9) most_common_price\n10) most_common_prices\n')
        user_input = input('What sorting method do you want?\n')
        logger.info('1) USD Price per VPND\n2) Total USD purchase price\n3) Total VPND\n4) Total AVAX\n')
        user_column = input('Which type of data do you want to sort?\n')

        sell_data, buy_data = api_manager \
            .get_data_manager() \
            .sort(get_column_by_num(user_column), user_input)

        sell_dict_arr = []
        for arr in sell_data:
            sell_dict_arr.append({'price': arr[0], 'times_found': arr[1]})

        buy_dict_arr = []
        for arr in buy_data:
            buy_dict_arr.append({'price': arr[0], 'times_found': arr[1]})

        logger.info(json.dumps(sell_dict_arr))
        logger.info('\n\n')
        logger.info(json.dumps(buy_dict_arr))

# logger.info(json.dumps(sell_data))
# logger.info('\n\n\n\n')
# logger.info(json.dumps(buy_data))
