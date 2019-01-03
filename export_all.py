import click
import re

import pymongo as pm

from datetime import datetime, timedelta
from web3 import Web3
from providers.auto import get_provider_from_uri
from exportblocks import ExportBlocks

@click.command(context_settings=dict(help_option_names=['-h', '--help']))

@click.option('-s', '--start', default=0, type=int, help='Start block')
@click.option('-e', '--end', required=True, type=int, help='End block')

@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')

def export_all(start,end,provider_uri):

    print("export_all")
    #建立数据库连接
    #conn = pm.MongoClient('mongodb://localhost:27017/')
    #db = conn.eth
    #db.authenticate("root","galaxy123456@")

    conn = pm.MongoClient('mongodb://172.17.156.121:27017/')
    db = conn.eth
    #db.authenticate("root","galaxy123456@")

    try:
        eb = ExportBlocks(start,end,get_provider_from_uri(provider_uri,batch=True),db)
        eb.start()

        #关闭数据库连接
        conn.close()
    except:
        conn.close()


export_all()
