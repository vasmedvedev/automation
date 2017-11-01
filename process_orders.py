#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
import xmlrpclib
from time import sleep
from config import config_from_file
from db import db_connection
from pba import PBAProxy


def get_poa_api_address(cursor):
    query = 'SELECT "PEMAddress", "PEMPort" FROM "PEMOptions"'
    cursor.execute(query)
    result = None

    for record in cursor.fetchall():
        ip_addr = ''.join(record['PEMAddress'].split())
        port = str(record['PEMPort'])
        result = 'http://{}:{}'.format(ip_addr, port)

    return result


def get_subscription(subscription_id, poa):
    params = {
        'subscription_id': int(subscription_id),
        'get_resources': False
    }
    res = poa.pem.getSubscription(params)
    return res


def create_subscription(acc_id, st_id, poa, sub_id=None):
    params = {
        'account_id': int(acc_id),
        'service_template_id': int(st_id),
    }

    if sub_id:
        params['subscription_id'] = int(sub_id)

    res = poa.pem.addSubscription(params)

    if res['status'] != 0:
        print("Subscription creation API failed: {}".format(res['error_message']), file=sys.stderr)
        sys.exit(1)

    return res


def remove_subscription(sub_id, poa):
    res = poa.pem.removeSubscription({'subscription_id': int(sub_id)})

    if res['status'] == 0:
        return res


def complete_oiid_with_trigger(sub_id, oiid, aid, st_id, poa, pba):
    create_subscription(aid, st_id, poa, sub_id)
    pba.trigger_event('Deletion Completed', oiid, sub_id, 'complete_oiid_with_trigger')
    remove_subscription(sub_id, poa)


def order_followup(order_id, db_conn, pba, poa, target_status=None):
    """
    :param order_id:
    :param db_conn:
    :type db_conn: db.db_connection
    :param pba:
    :type pba: pba.PBAProxy
    :param poa:
    :type poa: xmlrpclib.ServerProxy
    :param target_status:
    :return:
    :rtype: int
    """
    sleep_time = 5
    
    db_conn.cursor.execute("""
      SELECT s."Status", s."ServStatus", "subscriptionID", "OrderDocOrderID", "serviceTemplateID", "AccountID", "OIID"
      FROM "OItem" oi 
      JOIN "Subscription" s
      USING("subscriptionID") WHERE "OrderDocOrderID" = {}
      """.format(order_id)
    )

    data = db_conn.cursor.fetchall()
    
    for record in data:
        if record['ServStatus'] in (60, 70):
            db_conn.cursor.execute("""
                UPDATE "Subscription"
                SET "ServStatus" = 30
                WHERE "subscriptionID" = {}""".format(record['subscriptionID'])
            )
            if db_conn.cursor.rowcount == 1:
                print("Changing BA serv status of subscription {} to stopped in DB to resubmit order".format(record['subscriptionID']))
                db_conn.commit()
            else:
                print("Incorrect number of rows updated during fixing subscription status")
                db_conn.rollback()
                sys.exit(1)
                
        pba.restart_order(record['OrderDocOrderID'], target_status)
            
        print("Order {} was restarted".format(record['OrderDocOrderID']))
        
        sleep(sleep_time)
        
        order_status = pba.get_order_status(record['OrderDocOrderID'])
        subscr_check = get_subscription(record['subscriptionID'], poa)
        
        if order_status == 'CP':
            print("Order {} is completed".format(record['OrderDocOrderID']))
            print("______________")
            return 0
        elif order_status == 'PF':
            print("Order {} failed, will be checked with failed orders.".format(record['OrderDocOrderID']))
            print("______________")
            return 0
        elif subscr_check['status'] != 0 and "does not exist" in subscr_check['error_message'] and order_status == 'PR':
            print("Order was not completed. Will try to trigger event manually.")
            complete_oiid_with_trigger(record['subscriptionID'], record['OIID'], record['AccountID'], record['serviceTemplateID'], poa, pba)
            print("Event triggered. Subscription removed from POA.")
            print("______________")
            return 0
        else:
            if target_status == 'PD' and order_status == 'RB':
                print("Order {} dropped to CPC status will be checked in next block".format(record['OrderDocOrderID']))
                return 0
            print("Status {} is unexpected for order %s under account %s or subscription was not removed in OA please chek manually".format(order_status, record['OrderDocOrderID'], record['AccountID']))
            return -1


def process_cnbs_orders(db_conn, poa, pba):
    """
    :param db_conn:
    :type db_conn: db.db_connection
    :param pba
    :type pba: pba.PBAProxy
    :param poa:
    :type poa: xmlrpclib.ServerProxy
    :return:
    """
    print("Checking failed 'can not be stopped'/'Service_timeout' CL Orders")
    print("______________")
    db_conn.cursor.execute("""
        SELECT DISTINCT("subscriptionID"), "OrderDocOrderID", "OIID", s."Status", s."ServStatus", "ProcessingComment"
        FROM "OItem" io
        JOIN "Subscription" s
        USING("subscriptionID")
        WHERE "OrderDocOrderID"
        IN (SELECT "OrderID" FROM "SalesOrder" WHERE "OrderTypeID" = 'CF'
        AND "OrderStatusID" IN ('PF'))
        AND ("ProcessingComment" ~ 'Stopping service of Order Item' OR "ProcessingComment" ~ 'Service Creation Timeout Exceeded')
        AND s."Status" != 60 AND s."ServStatus" != 90"""
    )

    data = db_conn.cursor.fetchall()

    for record in data:
        subscription = get_subscription(record['subscriptionID'], poa)
        if subscription['status'] == 0:
            print("Order {} could not stop service of subscription {} will try to remove manually then resubmit and process the order.".format(record['OrderDocOrderID'], record['subscriptionID']))
            print("Removing Subscription {}...".format(record['subscriptionID']))
            try:
                remove_subscription(record['subscriptionID'], poa)
            except xmlrpclib.Fault as e:
                print("Failed to remove subscription:")
                print(str(e))
                print("Skipping Order")
                print("______________")
                continue
            print("Subscription %s successfully removed from OA".format(record['subscriptionID']))
        else:
            print("Order {} is failed despite subscription {} does not exist in OA will try resubmit and process it".format(record['OrderDocOrderID'], record['subscriptionID']))

        order_followup(record['OrderDocOrderID'], db_conn, pba, poa, 'PD')


def process_rb_orders(db_conn, poa, pba):
    """
    :param db_conn:
    :type db_conn: db.db_connection
    :param pba
    :type pba: pba.PBAProxy
    :param poa:
    :type poa: xmlrpclib.ServerProxy
    """

    print("Checking CPC CL Orders")
    print("______________")
    db_conn.cursor.execute("""
        SELECT DISTINCT ("subscriptionID"), "OrderDocOrderID", "OIID",
        s."Status", s."ServStatus", s."AccountID", s."serviceTemplateID"
        FROM "OItem" io
        JOIN "Subscription" s USING("subscriptionID")
        WHERE "OrderDocOrderID" IN
        (SELECT "OrderID" FROM "SalesOrder" WHERE "OrderTypeID" = 'CF' AND "OrderStatusID" IN ('RB'))
         AND s."Status" != 60 AND s."ServStatus" != 90
    """)
    data = db_conn.cursor.fetchall()
    for record in data:
        subscription = poa.get_subscription(record['subscriptionID'])
        if subscription['status'] != 0 and "does not exist" in subscription['error_message']:
            print("Found order %s in checking provisioning coditions without subscription in OA when it is supposed to be." % (record['OrderDocOrderID']))
            print("Adding subscription %s to operations for Order %s" % (record['subscriptionID'],record['OrderDocOrderID']))
            print("Creating subscription with parameters %s %s %s" % (record['AccountID'], record['serviceTemplateID'], record['subscriptionID']))
            poa.create_subscription(record['AccountID'], record['serviceTemplateID'], record['subscriptionID'])
            print("Restarting Order %s" % (record['OrderDocOrderID']))
            order_followup(record['OrderDocOrderID'], db_conn, pba, poa, 'I4')


def main():
    config = config_from_file('/usr/local/bm/etc/ssm.conf.d/global.conf')
    pba_proxy = xmlrpclib.ServerProxy('http://{}:5224/RPC2'.format(config.host_ip))
    pba = PBAProxy(pba_proxy)

    with db_connection(config) as db_conn:
        poa = xmlrpclib.ServerProxy(get_poa_api_address(db_conn.cursor))
        process_cnbs_orders(db_conn, poa, pba)
        process_rb_orders(db_conn, poa, pba)


if __name__ == "__main__":
    main()
