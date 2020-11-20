import json
from utils.json_rpc_requests import generate_get_block_by_number_json_rpc
from utils.utils import rpc_response_to_result
from mappers.block_mapper import EthBlockMapper
from mappers.transaction_mapper import EthTransactionMapper
from mappers.receipt_log_mapper import EthReceiptLogMapper
from mappers.token_transfer_mapper import EthTokenTransferMapper
from mappers.receipt_mapper import EthReceiptMapper
from mappers.contract_mapper import EthContractMapper
from service.eth_contract_service import EthContractService
from service.eth_token_service import EthTokenService
from mappers.token_mapper import EthTokenMapper


from utils.json_rpc_requests import generate_get_receipt_json_rpc
from utils.utils import rpc_response_batch_to_results
from utils.json_rpc_requests import generate_get_code_json_rpc

from exporters.blocks_and_transactions_item_exporter import blocks_and_transactions_item_exporter
from exporters.token_transfers_item_exporter import token_transfers_item_exporter
from exporters.receipts_and_logs_item_exporter import receipts_and_logs_item_exporter
from exporters.contracts_item_exporter import contracts_item_exporter
from exporters.tokens_item_exporter import tokens_item_exporter

from service.token_transfer_extractor import EthTokenTransferExtractor, TRANSFER_EVENT_TOPIC

class ExportBlocks():
    
    def __init__(
            self,
            start_block,
            end_block,
            web3_provider_batch,
            web3,
            db):

        self.start_block   = start_block
        self.end_block     = end_block
        self.web3_provider_batch = web3_provider_batch
        self.web3  = web3
        self.db            = db
        self.cur_block     = start_block
        self.tokens        = []
        #block
        self.block_item_exporter = blocks_and_transactions_item_exporter()
        self.block_item_exporter.open()

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()

        #token_transfer
        self.token_transfer_item_exporter = token_transfers_item_exporter()
        self.token_transfer_item_exporter.open()
        
        self.token_transfer_mapper = EthTokenTransferMapper()
        self.token_transfer_extractor = EthTokenTransferExtractor()
 
        #receipt
        self.receipts_and_logs_item_exporter = receipts_and_logs_item_exporter()
        self.receipts_and_logs_item_exporter.open()


        self.receipt_log_mapper = EthReceiptLogMapper()
        self.receipt_mapper = EthReceiptMapper()

        #contract
        self.contract_mapper = EthContractMapper()
        self.contract_service = EthContractService()
        self.contract_item_exporter = contracts_item_exporter()


        #tokens
        self.token_service = EthTokenService(self.web3,clean_user_provided_content)
        self.token_mapper = EthTokenMapper()
        self.tokens_item_exporter = tokens_item_exporter()
        self.tokens_item_exporter.open()

        self._export_tokens(self.tokens)


        print("ExportBlocks __init__")

    #导出block
    def start(self):
        print("ExportBlocks start")

        while(self.cur_block <= self.end_block) :
           self.export_block(self.cur_block)
           self.cur_block  = self.cur_block  + 1
           # print(self.cur_block)
         

    def export_block(self,blocknumber): 

        # print("export_block:",blocknumber) 
        # blockrpc = generate_get_block_by_number_json_rpc(blocknumber,True)
        # print(blockrpc)
        # response = self.web3_provider_batch.make_request(json.dumps(blockrpc)) 
        # result = rpc_response_to_result(response) 

        # #导出区块
        # block = self.block_mapper.json_dict_to_block(result)

        # #交易hash列表
        # trans_hashes = self._export_block(block)

        # #导出token_transfer
        # self._export_token_transfers(blocknumber)

        # #导出receipt
        # contract_addresses = self._export_receipts(trans_hashes)
        # contract_addresses=list(set(contract_addresses))

        contract_addresses = ['0x7ba9b94127d434182287de708643932ec036d365']


        #导出contracts
        self._export_contracts(contract_addresses)

     
    def _export_block(self, block):
    
        print("_export_block") 
        item = self.block_mapper.block_to_dict(block)
        ex = self.block_item_exporter.get_export(item)
        result = ex.get_content(item)
              
        try:
            self.db[ex.db_name].insert_one(result)
        except:
            # raise ValueError('Exporter for item insert_one')
            print('Exporter for export block insert_one')

        return self._export_transactions(block)


    def _export_transactions(self,block):
    
        print("_export_transaction")
        transaction_hash = []

        for tx in block.transactions:
            hash = self._export_transaction(tx)
            transaction_hash.append(hash)

        return transaction_hash

    def _export_transaction(self,tx):
        
        item = self.transaction_mapper.transaction_to_dict(tx)
        ex = self.block_item_exporter.get_export(item)
        result = ex.get_content(item) 

        try:
            self.db[ex.db_name].insert_one(result)
        except:
            # raise ValueError('Exporter for item insert_one')  
            print('Exporter for export transaction insert_one') 

        return item["hash"]     


    def _export_token_transfers(self,blocknumber):
        print("_export_token_transfer") 

        filter_params = {
            'fromBlock': blocknumber,
            'toBlock': blocknumber,
            'topics': [TRANSFER_EVENT_TOPIC]
        } 

        event_filter = self.web3_provider_batch.eth.filter(filter_params)
        events = event_filter.get_all_entries()
        
        token_transfers = []

        for event in events:
            # print(event)
            log = self.receipt_log_mapper.web3_dict_to_receipt_log(event)
            token_transfer = self.token_transfer_extractor.extract_transfer_from_log(log)
            self._export_token_transfer(token_transfer)

        self.web3_provider_batch.eth.uninstallFilter(event_filter.filter_id)

    def _export_token_transfer(self,token_transfer):
        
        if token_transfer is not None:
            item = self.token_transfer_mapper.token_transfer_to_dict(token_transfer)
            ex = self.token_transfer_item_exporter.get_export(item)
            result = ex.get_content(item)

            if(item["token_address"] not in self.tokens):
                self.tokens.append(item["token_address"])  

            try:
                self.db[ex.db_name].insert_one(result)
            except:
                # raise ValueError('Exporter for item insert_one')
                print('Exporter for export token transfer insert_one')                                 

    def  _export_receipts(self,transaction_hashes):
        print("_export_receipts")

        if transaction_hashes is None or len(transaction_hashes) == 0 : return []

        print('********************')



        receipts_rpc = list(generate_get_receipt_json_rpc(transaction_hashes))

        # print(receipts_rpc)

        # return


        response = self.web3_provider_batch.make_request(json.dumps(receipts_rpc))
        results = rpc_response_batch_to_results(response)

        receipts = [self.receipt_mapper.json_dict_to_receipt(result) for result in results]
        
        contract_addresses = []
        for receipt in receipts:
            ca = self._export_receipt(receipt)
            if(ca != None and len(ca) > 0):
                 contract_addresses = contract_addresses + ca
        return contract_addresses

    def _export_receipt(self, receipt):
        
        item = self.receipt_mapper.receipt_to_dict(receipt)

        ex = self.receipts_and_logs_item_exporter.get_export(item)
        result = ex.get_content(item) 

        try:
            self.db[ex.db_name].insert_one(result)
        except:
            # raise ValueError('Exporter for item insert_one')
            print('Exporter for export receipt insert_one')  

        return self._export_logs(receipt)


        # return item["contract-addresses"]
    
    def _export_logs(self,receipt):

        print("_export_logs")

        contract_addresses = []
        
        logs = []
        for log in receipt.logs:
            item = self.receipt_log_mapper.receipt_log_to_dict(log)
            ex   = self.receipts_and_logs_item_exporter.get_export(item)
            result = ex.get_content(item)  
            logs.append(result)
            contract_addresses.append(result["address"])
        # print(ex,ex.db_name)
        try:
            if len(logs) > 0:
                self.db[ex.db_name].insert_many(logs)
        except Exception as e:
            # raise ValueError('Exporter for item insert_one')
            print('Exporter for export logs insert_one')

        return  contract_addresses  


    def _export_contracts(self, contract_addresses):

        if contract_addresses == None or len(contract_addresses) == 0: return

        contracts_code_rpc = list(generate_get_code_json_rpc(contract_addresses))
        response_batch = self.web3_provider_batch.make_request(json.dumps(contracts_code_rpc))

        contracts = []
        for response in response_batch:
            # request id is the index of the contract address in contract_addresses list
            request_id = response['id']
            result = rpc_response_to_result(response)

            print(result)

            contract_address = contract_addresses[request_id]
            contract = self._get_contract(contract_address, result)
            contracts.append(contract)

        for contract in contracts:
            self._export_contract(contract)
        
   
    
    def _export_contract(self,contract):

        item = self.contract_mapper.contract_to_dict(contract)
        ex = self.contract_item_exporter.get_export(item)
        result = ex.get_content(item) 

        try:
            self.db[ex.db_name].insert_one(result)
        except:
            # raise ValueError('Exporter for item insert_one') 
            print('Exporter for export contract insert_one')    
            

    def _get_contract(self, contract_address, rpc_result):
        contract = self.contract_mapper.rpc_result_to_contract(contract_address, rpc_result)
        bytecode = contract.bytecode
        function_sighashes = self.contract_service.get_function_sighashes(bytecode)

        contract.function_sighashes = function_sighashes
        contract.is_erc20 = self.contract_service.is_erc20_contract(function_sighashes)
        contract.is_erc721 = self.contract_service.is_erc721_contract(function_sighashes)

        return contract   

    def _export_tokens(self, token_addresses):

        for token_address in token_addresses:
            self._export_token(token_address)

    def _export_token(self, token_address):

        token = self.token_service.get_token(token_address)
        item = self.token_mapper.token_to_dict(token)

        ex = self.tokens_item_exporter.get_export(item)
        result = ex.get_content(item) 

        try:
            self.db[ex.db_name].insert_one(result)
        except:
            # raise ValueError('Exporter for item insert_one')
            print('Exporter for export token insert_one')

    
ASCII_0 = 0

def clean_user_provided_content(content):
    
    if isinstance(content, str):
        # This prevents this error in BigQuery
        # Error while reading data, error message: Error detected while parsing row starting at position: 9999.
        # Error: Bad character (ASCII 0) encountered.
        return content.translate({ASCII_0: None})
    else:
        return content         





