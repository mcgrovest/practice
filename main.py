import requests
import time
import json
import hashlib
from getpass import getpass
import os
import configparser
import sys

template_mapNotifLast = ('http://{server}/kv/keys/mapNotifLast:{mapid}:{kv_session}'
                         '?n=3&r=2&w=2&waitTimeout=60&waitVersion={lastversion}')
template_mapNotif = ('http://{server}/kv/partition/mapNotif:{mapid}:{kv_session}'
                     '?from={fromtime}&n=3&r=2&w=2')
template_getnode = 'http://{server}/api/nodes/{nodeid}'
template_getmap = 'http://{server}/api/maps/{mapid}/export'
template_getbranch = 'http://{server}/api/maps/{mapid}/nodes/{root_id}'

pathname = os.path.dirname(sys.argv[0])
config = configparser.ConfigParser()

if sys.argv[1] == 'test':
    config.read(pathname + '\\test_config.ini')
    server = config['TEST']['server']
    username = config['TEST']['username']
    password = config['TEST']['password']
    mapid = config['TEST']['mapid']
    kv_session = config['TEST']['kv_session']
    node_id = config['TEST']['node_id']

elif sys.argv[1] == 'prod':
    config.read(pathname + '\\prod_config.ini')
    server = config['PROD']['server']
    username = config['PROD']['username']
    password = config['PROD']['password']
    mapid = config['PROD']['mapid']
    kv_session = config['PROD']['kv_session']
    node_id = config['PROD']['node_id']

lastversion = 0
fromtime = 0

def currenttime():  # форматированное время
    cur_time = time.time()
    cur_time *= 1000
    cur_time = round(cur_time)
    return(cur_time)

response_NotifLast = requests.get(template_mapNotifLast.format(server=server, mapid=mapid,
                                  kv_session=kv_session, lastversion=lastversion))
tmpp = response_NotifLast.json()
lastversion = tmpp.get("version")
fromtime = tmpp.get("value")

def change_node_status(branch,
                       cur_node,
                       changed_node,
                       user_changer):
    if (branch['body']['properties']['byType'] == user_changer) and (cur_node != changed_node):
        patch_body = {
            "properties": json.dumps({
                "byType": {
                    "Статус выполнения": "Приостановлено"
                }
            })
        }

        rqpatch = requests.patch(template_getnode.format(server=server, nodeid=cur_node),
                                 data=None, json=patch_body,
                                 auth=(username, password))


def change_branch_statuses(branch,
                           changed_node):
    cur_node = branch['body']['id']
    change_node_status(branch, cur_node, changed_node, user_changer)
    branch = branch['body']['children']
    branch = branch[0]
    if branch['body']['children'] == []:
        cur_node = branch['body']['id']
        change_node_status(branch, cur_node, changed_node, user_changer)
        return
    change_branch_statuses(branch, changed_node)


if __name__ == '__main__':
    while True:  # long poll
        response_NotifLast = requests.get(template_mapNotifLast.format(server=server, mapid=mapid,
                                                                       kv_session=kv_session,
                                                                       lastversion=lastversion))

        if response_NotifLast.status_code == 408:
            continue

        tmpp = response_NotifLast.json()
        lastversion = tmpp.get("version")

        if response_NotifLast.ok:
            response_Notif = requests.get(template_mapNotif.format(server=server, mapid=mapid,
                                                                   kv_session=kv_session,
                                                                   fromtime=fromtime))
            data = json.loads(response_Notif.content)

            for event in data:
                temp = (event['key'])
                fromtime = temp[0]
                user_changer = (event['value']['who']['username'])
                changed_node = (event['value']['what'])
                changed_status_response = json.loads(event['value']
                                                     ['data']['changes']['properties'])
                last_status = changed_status_response['byType']['Статус выполнения']

            if last_status == 'В работе':
                response_getbranch = requests.get(template_getbranch.format(server=server,
                                                                            mapid=mapid,
                                                                            root_id=node_id),
                                                  auth=(username, password))
                branch_json = json.loads(response_getbranch.content)
                branch = branch_json
                response_getexecutor = requests.get(template_getnode.format(server=server,
                                                                            nodeid=changed_node),
                                                    auth=(username, password))
                user_changer = json.loads(response_getexecutor.content)
                user_changer = user_changer['body']['properties']['byType']
                change_branch_statuses(branch, changed_node)
#  просто коммент чтобы закоммитить
