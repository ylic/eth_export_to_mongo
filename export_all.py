import click
import re

import pymongo as pm

from datetime import datetime, timedelta
from web3 import Web3
from providers.auto import get_provider_from_uri

@click.command(context_settings=dict(help_option_names=['-h', '--help']))

@click.option('-s', '--start-block', default=0, type=int, help='Start block')
@click.option('-e', '--end-block', required=True, type=int, help='End block')

@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
#@click.option('-o', '--output-dir', default='output', type=str, help='Output directory, partitioned in Hive style.')
def export_all():

    print("export_all")
    #建立数据库连接
    conn = pm.MongoClient('mongodb://localhost:27017/')
    db = conn.eth
    db.authenticate("root","galaxy123456@")

    eb = ExportBlocks(start-block,end-block,get_provider_from_uri(provider_uri),db)
    eb.start()

    #关闭数据库连接
    db.close()

export_all()
