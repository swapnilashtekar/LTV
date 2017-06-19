#!/usr/bin/python
import os, ast
import logging
from dateutil.parser import parse
from dateutil import rrule


"""
Logging
"""
logger = logging.getLogger('LTV Project')
#format = "%(asctime)s   [%(levelname)s] [%(funcName)s] [%(lineno)d] %(message)s"
format = "%(asctime)s   [%(levelname)s]  [%(lineno)d] %(message)s"
#logging.basicConfig(format=format, level=logging.WARNING)
logging.basicConfig(format=format, level=logging.INFO)

class LTV():

    def read_file(self, input_file, database_dict):
        logger.info("Input file is [{0}] and database dictionary is [{1}]".format(input_file, database_dict))
        first_line = True
        with open(input_file, 'r') as f_in:
            for line in f_in.readlines():
                if first_line:
                    first_line = False
                    payload = line.strip()[1:-1]
                else:
                    payload = line.strip()[:-1]

                logger.debug("Payload is : {0}".format(payload))

                """
                Call ingest function here
                """
                self.ingest(payload, database_dict)

    def ingest(self, e, D):

        logger.debug("Event is : {0} and Dictionary D is :{1}".format(e, D))
        payload_dict = ast.literal_eval(e)

        """
        Convert date into datetime format
        """
        if 'event_time' in payload_dict:
            payload_dict['event_time'] = parse(payload_dict['event_time'])
            logger.debug("Parsed date is {0} ".format(payload_dict['event_time']))

        """
        Extract customer id
        """
        if payload_dict['type'] == 'CUSTOMER':
            customer_id = payload_dict['key']
        elif payload_dict['type'] in ['SITE_VISIT', 'IMAGE', 'ORDER']:
            customer_id = payload_dict['customer_id']
        else:
            logger.warn("Unknown data type [{0}] in dictionary !".format(payload_dict['type']))

        logger.debug("Customer id is : {0}".format(customer_id))

        """
        Creating <Key, Value> store for Customer ID and the event data
        """
        if customer_id in D.keys():
            D[customer_id].append(payload_dict)
        else:
            D[customer_id] = [payload_dict]

    def TopXSimpleLTVCustomers(self, x, D):
        logger.debug("To find top {0} customers and database dictionary is {1}".format(x, D))

        lifetime_values = dict()

        for customer_id in D:
            """
            Appending event times to visits list
            """
            """
            Find SITE_VISIT to identify the visits to site
            If ORDER is present and no SITE_VISIT, in such cases the ORDER is considered as SITE_VISIT
            """
            visit_type = 'SITE_VISIT' if 'SITE_VISIT' in [ customer_id_events['type']
                                                          for customer_id_events in D[customer_id] ] else 'ORDER'
            visits = [ customer_id_events['event_time']
                      for customer_id_events in D[customer_id] if customer_id_events['type'] == visit_type ]


            if 'ORDER' in [customer_id_events['type'] for customer_id_events in D[customer_id]] and visits:
                weeks = (rrule.rrule(rrule.WEEKLY, dtstart=min(visits), until=max(visits))).count()
                logger.debug("Customer ID: {0} \t Number of weeks customer has been visiting: {1}".format(customer_id, weeks))

                """
                Gather order data in tuples
                """
                order_data = [(customer_id_events['key'], customer_id_events['verb'],
                               customer_id_events['event_time'],
                               float(customer_id_events['total_amount'].split()[0]))
                              for customer_id_events in D[customer_id] if customer_id_events['type'] == 'ORDER']
                logger.debug("Order data for customer is: {0}".format(order_data))

                order_amount_dict = dict()

                """
                To update the order in case of expenditure on later dates
                """
                for key, verb, event_date, total_amount in order_data:
                    if key in order_amount_dict:
                        if event_date > order_amount_dict[key][0]:
                            order_amount_dict[key] = (event_date, total_amount)
                    else:
                        order_amount_dict[key] = (event_date, total_amount)
                order_amount_total = []
                logger.debug("Order amount dictionary: {0}".format(order_amount_dict))

                for key in order_amount_dict:
                    order_amount_total.append(order_amount_dict[key][1])
                    # print "Appending %s to order amount total  %s" %(order_amount_total, key)

                """
                Customer expenditure per visit
                """
                expenditure_per_visit = float(sum(order_amount_total)) / weeks

                """
                Lifetime value: Simple LTV = 52 * a * t
                """
                t = 10
                lifetime_values[customer_id] = 52 * expenditure_per_visit * t

            else:
                """
                If Customer has no ORDER
                """
                lifetime_values[customer_id] = 0

        """
        Arrange dictionary in descending order
        """
        results = []
        lifetime_values_sorted_keys = sorted(lifetime_values, key=lifetime_values.get, reverse=True)
        for customer_id in lifetime_values_sorted_keys:
            results.append((customer_id, round(lifetime_values[customer_id],2)))

        return results[:x]


if __name__ == '__main__':

    ltv = LTV()

    database_dict = dict()

    input_file = '/'.join(['../input', 'input2.txt'])
    if os.path.isfile(input_file) and os.access(input_file, os.R_OK):
        logger.info("File exists and is readable!")
    else:
        logger.error("Either file does not exist or not readable!")

    """
    Read file
    """
    ltv.read_file(input_file, database_dict)

    logger.debug("Database dict after ingestion is: {0}".format(database_dict))
    x = 50
    results = ltv.TopXSimpleLTVCustomers(x, database_dict)

    """
    Write file
    """

    output_file = '/'.join(['../output', 'output2.txt'])
    logger.info("Writing top {0} customers with LTV to output file [{1}]".format(x, output_file))
    with open(output_file, 'w') as f_out:
        f_out.write('customer_id, LTV\n')
        for result in results:
            f_out.write(result[0] + ',' + str(result[1]) + '\n')