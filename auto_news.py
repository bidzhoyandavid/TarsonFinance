#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 13 11:21:52 2023

@author: david
"""

from config.config import *
from alpha_vantage.download import (
    alphaNews,
    alphaCompanyOverview,
    alphaListing,
    alphaStockPrices,
)
import pandas as pd
from tqdm import tqdm
import time
from alpha_vantage.get_functions import *
from alpha_vantage.transform import *
import time
from datetime import datetime, date, timedelta


previous_day = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


symbols = pd.read_sql(
    """
    select * from public.investment_company
    where
        1=1
        and status = 'Active'
   -- limit 1
    """,
    con=engine,
)

source_db = pd.read_sql(
    """
    select * from public.investment_sourcedomain
    """,
    con=engine,
)

category_db = pd.read_sql(
    """
    select * from public.investment_newscategory
    """,
    con=engine,
)

sentiment_db = pd.read_sql(
    """
    select * from public.investment_newssentiment
    """,
    con=engine,
)

topic_db = pd.read_sql(
    """
    select * from public.investment_newstopics
    """,
    con=engine,
)


error = []
non_200_status = []
zero_len = []

for i in symbols.index:
    symbol = symbols.loc[i, "symbol"]
    symbol_id = symbols.loc[i, "id"]

    try:
        data, code = alphaNews(symbol=symbol, limit=50)
    except:
        error.append(symbol)
        continue

    if code != 200:
        non_200_status.append(symbol)
        continue

    if len(data) == 0:
        zero_len.append(symbol)
        continue

    # data_yesterday = data[
    #     pd.to_datetime(data["time_published"]).dt.normalize() == previous_day
    # ]

    symbol_news = pd.read_sql(
        """
        select in1.* from public.investment_news in1
        join public.investment_newscompanyconnection incc
        on 1=1
        and in1.id = incc.news_id
        and incc.company_id = {}
        """.format(
            symbol_id
        ),
        con=engine,
    )

    unique = (
        pd.DataFrame(symbol_news["title"])
        .merge(data, indicator=True, how="right", on="title")
        .loc[lambda x: x["_merge"] == "right_only"]
    )
    unique["category_id"] = unique["category_within_source"].apply(
        lambda x: getIDNewsCategory(x, df=category_db)
    )
    unique["source_id"] = unique["source_domain"].apply(
        lambda x: getIDNewsSource(source=x, df=source_db)
    )
    unique["sentiment_id"] = unique["overall_sentiment_label"].apply(
        lambda x: getIDNewsSentiment(x, df=sentiment_db)
    )

    for n in unique.index:
        temp = unique[unique.index == n]

        topics = pd.DataFrame.from_dict(temp.loc[n, "topics"])
        ticker = pd.DataFrame.from_dict(temp.loc[n, "ticker_sentiment"])

        temp = temp[
            [
                "title",
                "url",
                "time_published",
                "summary",
                "overall_sentiment_score",
                "category_id",
                "source_id",
                "sentiment_id",
            ]
        ].rename(columns={"overall_sentiment_score": "score"})
        temp.to_sql(
            "investment_news",
            schema="public",
            if_exists="append",
            index=False,
            con=engine,
        )

        id_news = pd.read_sql(
            """
            select max(id) from public.investment_news
            """,
            con=engine,
        ).loc[0, "max"]
        if len(ticker) != 0:
            ticker["company_id"] = ticker["ticker"].apply(lambda x: getIDSymbol(x))
            ticker["ticker_sentiment_label_id"] = ticker[
                "ticker_sentiment_label"
            ].apply(lambda x: getIDNewsSentiment(sentiment=x, df=sentiment_db))
            ticker = ticker[
                [
                    "company_id",
                    "relevance_score",
                    "ticker_sentiment_score",
                    "ticker_sentiment_label_id",
                ]
            ]
            ticker["news_id"] = id_news
            ticker.columns = [
                "company_id",
                "relevance",
                "ticker_sentiment_score",
                "ticker_sentiment_label_id",
                "news_id",
            ]
            ticker.to_sql(
                "investment_newscompanyconnection",
                schema="public",
                if_exists="append",
                index=False,
                con=engine,
            )

        if len(topics) != 0:
            topics["news_id"] = id_news
            topics["topic_id"] = topics["topic"].apply(
                lambda x: getIDNewsTopics(
                    x.lower().replace(" - ", " ").replace(" ", "_"), df=topic_db
                )
            )
            topics = topics[["topic_id", "news_id", "relevance_score"]].rename(
                columns={"relevance_score": "relevance"}
            )
            topics.to_sql(
                "investment_newstopicsconnection",
                schema="public",
                if_exists="append",
                index=False,
                con=engine,
            )


print("The news for {} were uploaded successfully".format(previous_day))
