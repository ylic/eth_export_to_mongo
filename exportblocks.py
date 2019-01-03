import json
from utils.json_rpc_requests import generate_get_block_by_number_json_rpc
from utils.utils import rpc_response_to_result
from mappers.block_mapper import EthBlockMapper
from mappers.transaction_mapper import EthTransactionMapper
from mappers.receipt_log_mapper import EthReceiptLogMapper
from mappers.token_transfer_mapper import EthTokenTransferMapper

from exporters.blocks_and_transactions_item_exporter import blocks_and_transactions_item_exporter
from exporters.token_transfers_item_exporter import token_transfers_item_exporter

from service.token_transfer_extractor import EthTokenTransferExtractor, TRANSFER_EVENT_TOPIC

class ExportBlocks():
    
    def __init__(
            self,
            start_block,
            end_block,
            web3_provider,
            db):

        self.start_block   = start_block
        self.end_block     = end_block
        self.web3_provider = web3_provider
        self.db            = db
        self.cur_block     = start_block

        self.block_item_exporter = blocks_and_transactions_item_exporter()
        self.block_item_exporter.open()

        self.token_transfer_item_exporter = token_transfers_item_exporter()
        self.token_transfer_item_exporter.open()

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()
        self.receipt_log_mapper = EthReceiptLogMapper()
        self.token_transfer_mapper = EthTokenTransferMapper()

        self.token_transfer_extractor = EthTokenTransferExtractor()


        print("ExportBlocks __init__")

    #导出block
    def start(self):
        print("ExportBlocks start")

        while(self.cur_block <= self.end_block) :
           self.export_block(self.cur_block)
           self.cur_block  = self.cur_block  + 1
           print(self.cur_block)
         

    def export_block(self,blocknumber): 

        print("export_block:",blocknumber) 
        blockrpc = generate_get_block_by_number_json_rpc(blocknumber,True)
        print(blockrpc)
        response = self.web3_provider.make_request(json.dumps(blockrpc)) 
        result = rpc_response_to_result(response) 

        #导出区块
        block = self.block_mapper.json_dict_to_block(result)
        self._export_block(self.block_mapper.block_to_dict(block))

        #导出交易
        self._export_transaction(block)

        #导出token_transfer
        #self._export_token_transfer(blocknumber)

     
    def _export_block(self, item):
    
        print("_export_block") 
        ex = self.block_item_exporter.get_export(item)
        result = ex.get_content(item)
              
        try:
            self.db[ex.db_name].insert_one(result)
        except:
            raise ValueError('Exporter for item insert_one')

    def _export_transaction(self,item):
    
        print("_export_transaction")
        trans = []
        for tx in item.transactions:
            
            item = self.transaction_mapper.transaction_to_dict(tx)
            ex = self.block_item_exporter.get_export(item)
            result = ex.get_content(item)  

            trans.append(result) 

        try:
            self.db[ex.db_name].insert_many(trans)
        except:
            raise ValueError('Exporter for item insert_one')

    def _export_token_transfer(self,blocknumber):
        print("_export_token_transfer") 

        filter_params = {
            'fromBlock': blocknumber,
            'toBlock': blocknumber,
            'topics': [TRANSFER_EVENT_TOPIC]
        } 

        event_filter = self.web3_provider.eth.filter(filter_params)
        events = event_filter.get_all_entries()
        
        token_transfers = []
        for event in events:
            print(event)
            log = self.receipt_log_mapper.web3_dict_to_receipt_log(event)
            token_transfer = self.token_transfer_extractor.extract_transfer_from_log(log)
    
            if token_transfer is not None:
                item = self.token_transfer_mapper.token_transfer_to_dict(token_transfer)
                ex = self.token_transfer_item_exporter.get_export(item)
                result = ex.get_content(item)
                token_transfers.append(result)
        try:
            self.db[ex.db_name].insert_many(token_transfers)
        except:
            raise ValueError('Exporter for item insert_one')

        self.web3_provider.eth.uninstallFilter(event_filter.filter_id)




