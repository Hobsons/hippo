import logging
from data_sources.hippo_base import HippoDataSource


class SqlQuery(HippoDataSource):
    namespace = 'sqlquery'
    label = 'SQL Query'
    inputs = {
        'conn_string': {'input':'text','label':'JDBC Style Connection String'},
        'query': {'input':'text','label':'SQL Query'},
        'filter_field': {'input':'text','label':'Field For Filtering Old Rows'},
        'filter_val': {'input':'text','label':'Filter Field Starting Value','default':''}
    }

    def __init__(self, *args):
        super().__init__(*args, namespace=SqlQuery.namespace, inputs=SqlQuery.inputs)

    def process(self):
        if not self.conn_string or not self.query or not self.filter_field:
            return

        return