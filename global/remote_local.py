from tornado import httpclient
import json

class Worker:
    def __init__(self, ip):
        self.ip = ip

    async def submit(self, algorithm_name, step, table_id, params, schema, node_id):
        http_client = httpclient.AsyncHTTPClient()
        try:
            body = 'params={"algorithm":"'+algorithm_name+'","hash":"'+str(table_id)+'","step":'+str(step)+',"node_id":'+str(node_id)+',"schema":'+str(schema)+',"params":'+json.dumps(params)+'}'
            print(body)
            response = await http_client.fetch(self.ip, method="POST", body=body)
        except Exception as e:
            print("Error: %s" % e)
        else:
            print(response.body)



