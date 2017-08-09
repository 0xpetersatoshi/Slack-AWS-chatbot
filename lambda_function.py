from datetime import datetime, timedelta
import pg8000 as db
import os

# function that queries redshift table and returns a message
def redshift(client):

    # print retrieval message
    print("Retrieving Results...")

    # set date for query
    last_week = datetime.now() - timedelta(days=7)
    two_weeks = datetime.now() - timedelta(days=14)
    date = str(last_week.year) + "-" + str(last_week.month) + "-" + str(last_week.day)
    date2 = str(two_weeks.year) + "-" + str(two_weeks.month) + "-" + str(two_weeks.day)

    # build sql query
    query = "SELECT sum(pageviews) FROM {}.vw_final_channel_aggr_default where date >= \'{}\';".format(client, date)
    query2 = "SELECT sum(pageviews) FROM {}.vw_final_channel_aggr_default where date >= \'{}\';".format(client, date2)

    # connect to redshift
    conn = db.connect(database=os.environ['RS_DB'], host=os.environ['RS_HOST'], port=5439, user=os.environ['RS_USER'], password=os.environ['RS_PASSWORD'])
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()

    # insert metric into variable
    pgv = result[0]

    # running for 2nd metric
    cur.execute(query2)
    result = cur.fetchone()

    # insert metric into variable
    pgv2 = result[0]

    # conditional logic for output
    if pgv2 == 0 and pgv > 0:
        WoW = 1
    elif pgv2 == 0 and pgv == 0:
        WoW = 0
    else:
        WoW = (pgv - pgv2) / pgv2 * 100

    # return message to chat user
    msg = "{} had {} pageviews last week, which is a {}% change from the week prior.".format(client.capitalize(), pgv, round(WoW, 2))
    return msg


# function that sends response back
def build_response(message):

    # build JSON response
    return {
        "dialogAction" : {
            "type" : "Close",
            "fulfillmentState" : "Fulfilled",
            "message" : {
                "contentType" : "PlainText",
                "content" : message
            }
        }
    }

# lambda handler function
def lambda_handler(event, context):

    # ensure that it is the expected intent
    if "retrieveWeeklyInsight" == event["currentIntent"]["name"]:
        client = event["currentIntent"]["slots"]["client"]
        msg = redshift(client)
        print(msg) # for debugging purposes
        return build_response(msg)


print(redshift("enablon"))