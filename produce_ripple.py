import asyncio, os

from time import sleep
# from json import dumps
import simplejson as json
from kafka import KafkaProducer

kafka_host = os.environ.get("KAFKA_HOST", "kafka")
kafka_port = os.environ.get("KAFKA_PORT", "9092")
kafka_username = os.environ.get("KAFKA_USERNAME", None) # "kafka")
kafka_password = os.environ.get("KAFKA_PASSWORD", None) # "kafka")

kftopic1 = "gfs1" # config.get("kf_topic1", "gfs1")
kftopic2 = "gfs2" # config.get("kf_topic2", "gfs2")
kfgroup = "ripple-group" # config.get("kf_group", "ripple-group")

# 
# NODE EVENT message
# 
# {
#     "event": "create_node",
#     "node": {
#         "id": 1235,
#         "label": "label"
#     }
# }
# nodepayload = """
# {{
#     "event": "{}", 
#     "node": {{
#         "id": "{}", 
#         "label": "{}"
#     }}
# }}
# """

# 
# LINK EVENT message
# 
# {
#     "event": "create_link",
#     "link": {
#         "id": 1234,
#         "label": "label",
#         "outV": 1235,
#         "inV": 1236
#     },
#     "source": {
#         "id": 1235,
#         "label": "label"
#     },
#     "target": {
#         "id": 1236,
#         "label": "label"
#     }
# }
# linkpayload = """
# {
# }
# """

if __name__ == '__main__':

    print(str(kafka_host))
    print(str(kafka_port))

    print(str(kftopic1))
    print(str(kftopic2))
    print(str(kfgroup))

    producer = KafkaProducer(bootstrap_servers=[str(kafka_host) + ":" + str(kafka_port)], value_serializer=lambda x: x.encode('utf-8'))

    namespace = "gfs1"
    nodeevent = "create_node"
    # nodeid = "1859"
    nodeid = "113" # ipb155 -> rhel-ens19
    nodelabel = "Ip"

    # message = payload.format("1", "name" + "1")
    # message = nodepayload.format(nodeevent, nodeid, nodelabel)
    # print(message)
    producer.send(kftopic1, key=bytes(str(nodeid), 'utf-8'), value=json.dumps({
        "namespace": namespace, 
        "event": nodeevent, 
        "chain": [], 
        "node": {
            "id": nodeid, 
            "label": nodelabel
        }
    }))
    sleep(1)

    # message = payload.format("2", "name" + "2")
    # producer.send(kftopic2, key=bytes("2", 'utf-8'), value=message)
    # sleep(1)

    print('done')
