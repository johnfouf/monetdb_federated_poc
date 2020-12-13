from tornado import httpclient
import json

class Worker:
    def __init__(self, ip):
        self.ip = ip

    async def submit(self, algorithm_name, step, table_id, params, schema):
        http_client = httpclient.AsyncHTTPClient()
        try:
            body = f'algorithm={algorithm_name}&hash={table_id}&step={step}&schema={schema}&params={json.dumps(params)}'
            response = await http_client.fetch(self.ip, method="POST", body=body)
        except Exception as e:
            print("Error: %s" % e)
        else:
            print(response.body)



