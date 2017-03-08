import logging
import json
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

    def parse_conn_str(self):
        protocol, rest = self.conn_string.split('://')
        protocol = protocol.replace('jdbc:','')
        userpass, rest = rest.split('@')
        hostport, schema = rest.split('/')
        args = {
            'database': schema,
            'host': hostport.split(':')[0],
            'user': userpass.split(':')[0],
            'password': userpass.split(':')[1]
        }
        if ':' in hostport:
            args['port'] = int(hostport.split(':')[1])
        return protocol, args


    def process(self):
        if not self.conn_string or not self.query or not self.filter_field or self.new_task_limit < 1:
            return

        try:
            protocol, connect_args = self.parse_conn_str()
        except Exception as e:
            logging.warning("SQL Data Source Conn String Parse Error: " + self.conn_string)
            logging.warning(e)
            return

        if protocol == 'mysql':
            from mysql.connector import connect
            conn = connect(**connect_args)
        elif protocol == 'postgresql':
            from psycopg2 import connect
            conn = connect(**connect_args)
        else:
            logging.warning("Unsupported SQL DB Protocol: " + protocol)
            return

        query = self.query
        if self.filter_val:
            if 'WHERE' not in query.upper():
                query += ' WHERE '
            else:
                query += ' AND '
            try:
                fv = int(self.filter_val)
                query += self.filter_field + ' > ' + str(fv)
            except Exception:
                fv = self.filter_val
                query += self.filter_field + " > '" + fv + "'"
        query += ' ORDER BY ' + self.filter_field
        query += ' LIMIT ' + str(self.new_task_limit)

        cursor = conn.cursor()
        cursor.execute(query)
        filter_field_index = None
        for idx, col in enumerate(cursor.description):
            if col[0] == self.filter_field:
                filter_field_index = idx
        tasks = []
        for row in cursor:
            tasks.append(json.dumps(row))
            if filter_field_index is not None:
                self.hippo_queue.definition['queue'][self.namespace]['filter_val'] = row[filter_field_index]

        self.create_tasks(tasks)
