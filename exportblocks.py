import json
from utils.json_rpc_requests import generate_get_block_by_number_json_rpc
from utils.utils import rpc_response_to_result
from mappers.block_mapper import EthBlockMapper
from mappers.transaction_mapper import EthTransactionMapper
from exporters.blocks_and_transactions_item_exporter import blocks_and_transactions_item_exporter


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

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()

        print("ExportBlocks __init__")

    #导出block
    def start(self):
        print("ExportBlocks start")

        while(self.cur_block <= self.end_block) :
           self.export_block(self.cur_block)
           self.cur_block  = self.cur_block  + 1
         

    def export_block(self,blocknumber): 

        print("export_block:",blocknumber) 
        blockrpc = generate_get_block_by_number_json_rpc(blocknumber,True)
        print(blockrpc)
        response = self.web3_provider.make_request(json.dumps(blockrpc)) 
        result = rpc_response_to_result(response) 

        block = self.block_mapper.json_dict_to_block(result)
        item = self.block_mapper.block_to_dict(block)
        print(result) 
        
        self._export_block(item)
            	 

    def _export_block(self, block):
    
        print("_export_block") 
        ex = self.block_item_exporter.get_export(block)
        result = ex.get_content(block)
              
        try:
            self.db[ex.db_name].insert_one(result)
        except:
            raise ValueError('Exporter for item insert_one')


