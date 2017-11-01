import re
import hashlib

from abstract import Proxy


class PBAProxy(Proxy):

    def get_order_status(self, order_id):
        response = self.proxy.Execute({
            'methodName': 'Execute',
            'Server': 'BM',
            'Method': 'GetOrder_API',
            'Params': [order_id]
        })
        return response['Result'][0][4]

    def get_order_signature(self, order_id):
        """
            Makes order signature for given order_id
            :param order_id: Order ID
            :type order_id: int, long
            :return: signature
            :rtype: str
            """
        response = self.proxy.Execute({
            'methodName': 'Execute',
            'Server': 'BM',
            'Method': 'GetOrder_API',
            'Params': [order_id]
        })

        result = response['Result']

        order_id = str(result[0][0])
        order_number = str(result[0][1])
        creation_time = str(result[0][6])
        order_total = str(result[0][8])
        description = str(result[0][12])
        currency = result[0][-2]
        precision = 2

        try:
            with open('/usr/local/stellart/share/currencies.txt', 'r') as settings_file:
                for line in settings_file:
                    if re.match(currency, line):
                        precision = int(line.split()[2])
        except (IOError, ValueError):
            pass

        if re.match(r'\d{,10}\.\d{1}$', order_total) is not None:
            order_total += '0' * (precision - 1)
        else:
            order_total_regex = '\d{,10}\.?\d{,%s}' % precision
            order_total = re.search(order_total_regex, order_total).group()

        # Concatenate signature parts
        signature_part1 = ''.join([order_id, order_number, creation_time, currency])
        signature_part2 = ''.join([order_total, description])

        # Truncate space at the end
        signature = ' '.join([signature_part1, signature_part2]).rstrip()

        # Generate md5sum
        sigres = hashlib.md5(signature.encode('utf-8')).hexdigest()

        return sigres

    def order_status_change(self, order_id, order_signature, status='PD'):
        response = self.proxy.Execute({
            'methodName': 'Execute',
            'Server': 'BM',
            'Method': 'OrderStatusChange_API',
            'Params': [order_id, status, order_signature]
        })
        return response

    def restart_order(self, order_id, target_status='PD'):
        signature = self.get_order_signature(order_id)
        return self.order_status_change(order_id, signature, target_status)

    def trigger_event(self, ekid, oiid, sid, message='EventProcessing'):

        params_map = {
            'Creation Completed': 'OrderItemID={}; SubscrID={}; IssuedSuccessfully=1; Message={}'.format(oiid, sid, message),
            'Deletion Completed': 'OrderItemID={}; IssuedSuccessfully=1; Message={}.'.format(oiid, message)
        }

        params = params_map.get(ekid)

        if not params:
            return None

        res = self.proxy.Execute({
            'methodName': 'Execute',
            'Server': 'TASKMAN',
            'Method': 'PostEvent',
            'Params': [ekid, params, 0]
        })
        return res
