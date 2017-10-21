from __future__ import print_function
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os

# TODO 
# add more context to response, i.e. 
# ...had x pageviews last week between Tuesday-Saturday

# create credentials object
credentials = {
    'user': os.environ['USER'],
    'password': os.environ['PASSWORD'],
    'host': os.environ['HOST'],
    'database': os.environ['DATABASE']
}

# narrative object that will pull different versions for the output
narrative = {
    'story': [
        """After spend {desc_spend_past} by *{spend:.2%}* week over week for _{client}_, \
         macro goals saw a {desc_goals_pres} to {goal_last_week:,} which was a *{goals:.2%}* \
         {desc_goals_pres} from the week prior. Similarly, pageviews {desc_pg_past} by \
          *{pageviews:.2%}* to {pg_last_week:,} for the week with {imp_last_week:,} \
          impressions during that timeframe.""",
        """_{client}_ had {pg_last_week:,} pageviews last week, which was a *{pageviews:.2%}* \
        {desc_pg_pres} from the week prior. Impressions {desc_imp_past} by *{impressions:.2%}* \
        to {imp_last_week:,}. Goals {desc_goals_past} by *{goals:.2%}* to {goal_last_week:,} and \
        were mainly driven by the _{leading_campaign}_ campaign. Finally, spend {desc_spend_past} \
        to ${spend_last_week:,}, which was a change of *{spend:.2%}* from the previous week."""
    ]
}

# nlg dictionary that stores synonyms in different tenses for words to be inserted into the narrative
nlg_dict = {
    'positive_pres': ['increase', 'climb', 'rise', 'boost', 'expansion', 'gain', 'hike', 'surge'],
    'negative_pres': ['drop', 'decrease', 'fall', 'decline', 'downturn', 'reduction'],
    'positive_past': ['grew', 'increased', 'climbed', 'elevated', 'enlarged', 'appreciated', 'extended', 'expanded', 'rose'],
    'negative_past': ['dropped', 'fell', 'decreased', 'declined', 'dropped off', 'lowered', 'depreciated', 'diminished']
}


def redshift(client):
    """Runs the SQL query in Redshift and returns a formatted message.
    
    Args:
        client: The client name to pass into the SQL query.

    Returns:
        The formatted message that will eventually be passed back to the user in Slack.
    """

    try:

        # create time object to determine query time
        last_week = datetime.now() - timedelta(days=7)
        two_weeks = datetime.now() - timedelta(days=14)

        # set date for query
        date = "{:%Y-%m-%d}".format(last_week)
        date2 = "{:%Y-%m-%d}".format(two_weeks)

        # build sql query
        query = '''SELECT 
            sum(pageviews) as "pageviews", 
            sum("Paid Total Impressions") as "impressions", 
            sum("Goal Macro Completions") as "macro_goals", 
            sum("Paid Total Clicks") as "clicks", 
            sum("Paid Total Cost") as "spend" 
            FROM {}.vw_final_channel_aggr_default 
            where date >= {!r};'''.format(client, date)

        # for debugging
        print('query: ', query)

        # build 2nd sql query
        query2 = '''SELECT 
            sum(pageviews) as "pageviews", 
            sum("Paid Total Impressions") as "impressions", 
            sum("Goal Macro Completions") as "macro_goals", 
            sum("Paid Total Clicks") as "clicks", 
            sum("Paid Total Cost") as "spend" 
            FROM {}.vw_final_channel_aggr_default 
            where date >= {!r};'''.format(client, date2)

        # build 3rd sql query
        query3 = '''SELECT TOP 1 "campaign name" as campaign_name, sum("Goal Macro Completions") as "macro_goals"
            FROM {}.vw_final_channel_aggr_default 
            WHERE date >= {!r}
            GROUP BY "campaign name"
            ORDER BY sum("Goal Macro Completions") DESC;'''.format(client, date)

        # format db connection url
        db_url = 'redshift+psycopg2://{user}:{password}@{host}:5439/{database}'.format(**credentials)

        # for debugging
        print('DB URL: ', db_url)

        # create connection engine
        engine = create_engine(db_url)

        # execute query and store records in DataFrame
        df = pd.read_sql_query(query, engine)
        df2 = pd.read_sql_query(query2, engine)
        df3 = pd.read_sql_query(query3, engine)

        # pageview week over week calculations
        pg1 = df['pageviews'][0]

        print("pageviews df: ", pg1)

        pg2 = df2['pageviews'][0]

        print("pageviews df2: ", pg2)

        wow_pg = (pg1 - pg2) / pg2.astype(float)

        print("Week over week: ", wow_pg)

        # impression week over week calculations
        imp1 = df['impressions'][0]
        imp2 = df2['impressions'][0]

        wow_imp = (imp1 - imp2) / imp2.astype(float)

        # goals week over week calculations
        goal1 = df['macro_goals'][0]
        goal2 = df2['macro_goals'][0]

        wow_goals = (goal1 - goal2) / goal2.astype(float)

        # clicks week over week calculations
        click1 = df['clicks'][0]
        click2 = df2['clicks'][0]

        wow_clicks = (click1 - click2) / click2.astype(float)

        # spend week over week calculations
        spend1 = df['spend'][0]
        spend2 = df2['spend'][0]

        wow_spend = (spend1 - spend2) / spend2.astype(float)

        # leading campaign
        leading_campaign = df3['campaign_name'][0]

        # create lambda function for future use
        key_selector_pres = lambda x: 'negative_pres' if x < 0 else 'positive_pres' if x > 0 else 'no_change'
        key_selector_past = lambda x: 'negative_past' if x < 0 else 'positive_past' if x > 0 else 'no_change'

        # conditional logic for determining descriptive words
        key_pg = key_selector_pres(wow_pg)
        key_pg2 = key_selector_past(wow_pg)

        key_imp = key_selector_past(wow_imp)

        key_spend = key_selector_past(wow_spend)

        key_goal = key_selector_past(wow_goals)
        key_goal2 = key_selector_pres(wow_goals)

        message_variables = {
            'client': client.capitalize(),
            'pg_last_week': pg1,
            'pageviews': wow_pg,
            'imp_last_week': imp1,
            'impressions': wow_imp,
            'goal_last_week': goal1,
            'goals': wow_goals,
            'click_last_week': click1,
            'clicks': wow_clicks,
            'spend_last_week': spend1,
            'spend': wow_spend,
            'leading_campaign': leading_campaign,
            'desc_pg_pres': nlg_dict[key_pg][np.random.randint(len(nlg_dict[key_pg]))],
            'desc_pg_past': nlg_dict[key_pg2][np.random.randint(len(nlg_dict[key_pg2]))],
            'desc_imp_past': nlg_dict[key_imp][np.random.randint(len(nlg_dict[key_imp]))],
            'desc_spend_past': nlg_dict[key_spend][np.random.randint(len(nlg_dict[key_spend]))],
            'desc_goals_past': nlg_dict[key_goal][np.random.randint(len(nlg_dict[key_goal]))],
            'desc_goals_pres': nlg_dict[key_goal2][np.random.randint(len(nlg_dict[key_goal2]))]
        }

        # create random index to pull random narrative version
        idx = np.random.randint(len(narrative['story']))
        print("index: ", idx)
        print(narrative['story'][idx])

        # construct message to be sent back to slack user
        msg = narrative['story'][idx].format(**message_variables)

        # for debugging
        print('-' * 25)
        print('message: ', msg)
        print('-' * 25)
        print(df)
        print('-' * 25)
        print(df2)
        
        return msg
        
    except Exception as e:

        # for debugging
        print('Error: ', e)

        # message if error
        msg = 'Sorry, it doesn\'t look like we have data for {} in the requested format.'.format(client.capitalize())

        # for debugging
        print('message: ', msg)

        return msg


def build_response(message):
    """This function takes the output of the redshift() function, inserts in into json format
    and sends it back to Amazon Lex which forwards it back to the Slack chat.

    Args:
        message: This is the output of redshift().

    Returns:
        A json object that contains the response to the user's query to be consumed by Amazon Lex.
    """

    # build JSON response
    return {
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": {
                "contentType": "PlainText",
                "content": message
            }
        }
    }


def lambda_handler(event, context):
    """Handles the incoming events from Amazon Lex and executes the other functions if the correct intent is determined.

    Args:
        event: Incoming event from Amazon Lex.
        context: Unused argument.

    Returns:
        Executes the build_response() function which returns the message to the user.
    """

    # ensure that it is the expected intent
    if "retrieveWeeklyInsight" == event["currentIntent"]["name"]:
        
        # extract client name from lex
        client = event["currentIntent"]["slots"]["client"]
        
        # for debugging
        print('client name: ', client)
        
        # run redshift function that will return message
        msg = redshift(client)

        # for debugging
        print('message passed to lambda: ', msg)

        return build_response(msg)