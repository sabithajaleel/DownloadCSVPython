import pandas as pd
from azure.cosmosdb.table.tableservice import TableService

SOURCE_TABLE = "BookingResponse"
databaseName = "" """ Set the Database Name """
databaseKey = "" """ Set the Database Key of Azure Table storage """
fileName = "" """ Set the CSV filename with path """

def set_table_service():
    """ Set the Azure Table Storage service """
    return TableService(account_name=databaseName, account_key=databaseKey)

def get_dataframe_from_table_storage_table(table_service, filter_query):
    """ Create a dataframe from table storage data """
    return pd.DataFrame(get_data_from_table_storage_table(table_service,
                                                          filter_query))

def get_data_from_table_storage_table(table_service, filter_query):
    """ Retrieve data from Table Storage """
    keyMarkers = {}
    keyMarkers['nextpartitionkey'] = 0
    keyMarkers['nextrowkey'] = 0
    b=[]
    while True:
        #get a batch of data
        a = table_service.query_entities(table_name=SOURCE_TABLE, filter=filter_query,num_results=1000 ,marker=keyMarkers)
        #copy results to list
        for item in a.items:
            b.append(item)
        #check to see if more data is available
        if len(a.next_marker) == 0:
            del a
            break
        #if more data available setup current position
        keyMarkers['nextpartitionkey'] = a.next_marker['nextpartitionkey']
        keyMarkers['nextrowkey'] = a.next_marker['nextrowkey'] 
        #house keep temp storage
        del a
    #return final list
    return b 
    
fq = "PartitionKey eq 'BookingResponse'"
ts = set_table_service()
df = get_dataframe_from_table_storage_table(table_service=ts,
                                            filter_query=fq)
#print(df)
export_csv = df.to_csv (fileName, index = None, header=True)                                            