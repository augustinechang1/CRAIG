import pandas as pd
import bs4
from bs4 import BeautifulSoup
import urllib.request
import time
import random
import requests
from datetime import date, timedelta
import datetime as dt
import json
import os
import uuid
from time import sleep

import requests
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup

nyc_list = "https://www.yelp.com/biz/starbright-floral-design-new-york-6"




def rand_user_agent(**kwargs):
    user_agent_list = [
        #Chrome
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
        "Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
        #Firefox
        "Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)",
        "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
        "Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)",
        "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko",
        "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)",
        "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)",
        "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729"
    ]

    return random.choice(user_agent_list)


def yelp_scraper(**kwargs):
    
    date_check = str(date.today() - timedelta(days=7))

    a=[] #author list
    rv=[] #rating value list
    dp=[] #date published list
    d=[] #review list
    r_url=[] #reviewer list 

    #initial request to get vendor name and address
    header1 = {'User-Agent': rand_user_agent()}
    request1 = urllib.request.Request(nyc_list,headers=header1)
    response1 = urllib.request.urlopen(request1)
    html1 = response1.read()
    soup1 = BeautifulSoup(html1, "html.parser")

    reviewCount = int(soup1.findAll('span', {'itemprop': 'reviewCount'})[0].text)
    vendor_name = soup1.findAll('meta', {'itemprop': 'name'})[1]['content']

    city = soup1.find('span', {'itemprop':'addressLocality'}).text
    state = soup1.find('span', {'itemprop':'addressRegion'}).text
    postalCode = soup1.find('span', {'itemprop':'postalCode'}).text

    try:
        streetAddress = soup1.find('span', {'itemprop':'streetAddress'}).text
    except:
        streetAddress = None

    #iterate through each page of reviews 
    for i in range(0, reviewCount, 20):

        url =nyc_list+'?start={}&sort_by=date_desc'.format(i)
        header = {'User-Agent': rand_user_agent()}
        request = urllib.request.Request(url,headers=header)
        response = urllib.request.urlopen(request)
        html = response.read()
        soup = BeautifulSoup(html, "html.parser")

        #contains all authors, rating, dates and reviews on the page 
        author = soup.findAll('meta', {'itemprop': 'author'})
        ratingValue = soup.findAll('div', {'itemprop':'review'})
        datePublished = soup.findAll('meta', {'itemprop': 'datePublished'})
        description = soup.findAll('div', {'itemprop':'review'})

        #unique user IDs 
        reviewer_url = soup.body.find('div', class_ = 'lemon--div__373c0__1mboc spinner-container__373c0__N6Hff border-color--default__373c0__2oFDT')
        lemon = 'lemon--div__373c0__1mboc sidebarActionsHoverTarget__373c0__2kfhE arrange__373c0__UHqhV gutter-12__373c0__3kguh grid__373c0__29zUk layout-stack-small__373c0__3cHex border-color--default__373c0__2oFDT'
        try: 
            lemon_ = reviewer_url.findAll('div', class_ = lemon)
        except:
            lemon_ = None
        
        if datePublished[0]['content'] > date_check:
        
            #iterate through each review on the page (max 20)
            for j in range(0,20):

                if datePublished[j]['content'] > date_check:

                    try:
                        desc = description[j].findAll('p', {'itemprop':'description'})
                        d.append(desc[0].text)
                        a.append(author[j]['content'])
                        rv.append(ratingValue[j].findAll('meta')[1]['content'])
                        dp.append(datePublished[j]['content'])
                        try:
                            r_url.append(lemon_[j].a['href'])
                        except:
                            r_url.append(None)
                    except:
                        break
                else:
                    break
        else:
            break
            
    df = pd.DataFrame({'vendor': vendor_name,
                        'streetAddress': streetAddress,
                        'city': city,
                        'state': state,
                        'postalCode': postalCode,
                        'author':a, 
                        'ratingValue':rv, 
                        'datePublished':dp, 
                        'description': d, 
                        'author_url': r_url
                        })

    df['ratingValue'] = pd.to_numeric(df['ratingValue'])
    df['postalCode'] = pd.to_numeric(df['postalCode'])
    # df['datePublished'] = pd.to_datetime(df['datePublished'], format='%Y-%m-%d')
    df = df.sort_values(['datePublished'], ascending=False)
    df.reset_index(inplace=True)
    df.drop(['index'], axis=1,inplace=True)

    return json.dumps(list(df.T.to_dict().values()))

# def list_scrape(url_list, **kwargs):

#     df = pd.DataFrame()

#     for i in range(len(url_list)):
#         vendor = yelp_scraper(url_list[i])
#         df = df.append([vendor])
    
#     df.reset_index(inplace=True)
#     df.drop(['index'], axis=1, inplace=True)

#     return df

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2018, 10, 3, 15, 58, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('scraping_reviews',
         catchup=False,
         default_args=default_args,
        #  schedule_interval='*/2 * * * *',
         # schedule_interval=None,
         ) as dag:

    opr_scrape_reviews = PythonOperator(task_id='list_scrapes',
                                        python_callable=yelp_scraper, provide_context=True)

    # opr_email = EmailOperator(
    #     task_id='send_email',
    #     to='augustine@yourgesture.com',
    #     subject='Airflow Finished',
    #     html_content=""" <h3>DONE</h3> """,
    #     dag=dag
    # )