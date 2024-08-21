import requests
import json
import os
import csv
import re
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# variable to set s3prefix by getting the current time and using a project name
project_name = 'ali_crawler'
date_prefix = datetime.now().strftime("%Y-%m-%d")
s3_prefix = f'{project_name}/{date_prefix}'

# Check if the environment variables exist
API_USER = os.environ.get('API_USER')
API_PASSWORD = os.environ.get('API_PASSWORD')

if API_USER is None or API_PASSWORD is None:
    raise EnvironmentError("API_USER and/or API_PASSWORD environment variables are not set.")

# API CREDENTIALS
API_credentials = (API_USER, API_PASSWORD)

# Function to clean list values
def clean_item(row):
    for key in row.index:
        if isinstance(row[key], list):
            row[key] = ', '.join(map(str, row[key]))
    return row

def get_top_selling_products(**kwargs):
    url = 'https://www.aliexpress.com/p/calp-plus/index.html?spm=a2g0o.categorymp.allcategoriespc.5.1f1aHVKoHVKoWF&categoryTab=consumer_electronics'
    payload = {
        'source': 'universal_ecommerce',
        'url': url,
        'geo_location': 'Nigeria',
        'locale': 'en-us',
        'user_agent_type': 'desktop',
        'render': 'html',
        'browser_instructions': [{'type': 'scroll', 'x': 0, 'y': 2400, 'wait_time_s': 2}] * 19,
        'parse': True,
        'parsing_instructions': {
            'products': {
                '_fns': [
                    {'_fn': 'xpath',
                        '_args': ['//div[@data-spm="prodcutlist"]/div']
                    }
                ],
                '_items': {
                    'Title': {
                        '_fns': [
                            {
                                '_fn': 'xpath_one',
                                '_args': ['.//h3/text()']
                            }
                        ]
                    },
                    'Price current': {
                        '_fns': [
                            {
                                '_fn': 'xpath',
                                '_args': ['.//div[@class="U-S0j"]']
                            }
                        ],
                        '_items': {
                            '_fns': [
                                {'_fn': 'xpath', '_args': ['.//span/text()']},
                                {'_fn': 'join', '_args': ''}
                            ]
                        }
                    },
                    'Price original': {
                        '_fns': [
                            {
                                '_fn': 'xpath_one',
                                '_args': ['.//div[@class="_1zEQq"]/span/text()']
                            }
                        ]
                    },
                    'Sales amount': {
                        '_fns': [
                            {
                                '_fn': 'xpath_one',
                                '_args': ['.//span[@class="Ktbl2"]/text()']
                            }
                        ]
                    },
                    'URL': {
                        '_fns': [
                            {
                                '_fn': 'xpath_one',
                                '_args': ['.//a/@href']
                            },
                            {
                                '_fn': 'regex_find_all',
                                '_args': [r'^\/\/(.*?)(?=\?)']
                            }
                        ]
                    }
                }
            }
        }
    }

    response = requests.post('https://realtime.oxylabs.io/v1/queries', auth=API_credentials, json=payload)


    # Debugging response content
    print("Response status code:", response.status_code)
    if response.status_code != 200:
        print("Response content:", response.content)
        response.raise_for_status()

    data = []

    response_page = response.json()
    
    print('Response Geenrated and stored')

    # extending the empty data list with the list of response values
    data.extend(response_page['results'][0]['content']['products'])


    products_url = [url for item in data if 'URL' in item for url in item['URL']]

    # Save URLs
    urlFile_path = "/opt/airflow/data/main/top_selling_products_urls.json"
    os.makedirs(os.path.dirname(urlFile_path), exist_ok=True)
    with open(urlFile_path, "w") as ufile:
        json.dump(products_url, ufile, indent=4)

    print('Products Urls Extracted and stored')

    # DATAFRAME
    df = pd.DataFrame(data)
    # Apply the function to each row
    df = df.apply(clean_item, axis=1)
    # Define file path
    file_path = "/opt/airflow/data/main/top_selling_products.csv"
    # Save DataFrame to CSV
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False, encoding='utf-8')

    # Return the product URLs for the next tasks
    return products_url

def get_product_information(**kwargs):
    # Retrieve the product URLs from XCom
    ti = kwargs['ti']
    product_urls = ti.xcom_pull(task_ids='extract_top_selling_products')

    payload = {
        'source': 'universal_ecommerce',
        'url': None,
        'geo_location': 'Nigeria',
        'locale': 'en-us',
        'user_agent_type': 'desktop',
        'render': 'html',
        'browser_instructions': [{'type': 'click', 'selector': {'type': 'xpath', 'value': '//div[@data-pl="product-specs"]//button'}}],
        'parse': True,
        'parsing_instructions': {
            'Title': {
                '_fns': [{
                    '_fn': 'xpath_one',
                    '_args': ['//h1[@data-pl="product-title"]/text()']
                }]
            },
            'Price current': {
                '_fns': [{
                    '_fn': 'xpath',
                    '_args': ['//div[contains(@class, "product-price-current")]']
                }],
                '_items': {
                    '_fns': [
                        {'_fn': 'xpath', '_args': ['.//span/text()']},
                        {'_fn': 'join', '_args': ''}
                    ]
                }
            },
            'Price original': {
                '_fns': [{
                    '_fn': 'xpath_one',
                    '_args': ['//span[contains(@class, "price--original")]/text()']
                }]
            },
            'Discount': {
                '_fns': [{
                    '_fn': 'xpath_one',
                    '_args': ['//span[contains(@class, "price--discount")]/text()']
                }]
            },
            'Sold': {
                '_fns': [
                    {
                        '_fn': 'xpath_one',
                        '_args': ['//div[@data-pl="product-reviewer"]//span[contains(text(), "sold")]/text()']
                    },
                    {'_fn': 'amount_from_string'}
                ]
            },
            'Rating': {
                '_fns': [
                    {
                        '_fn': 'xpath_one',
                        '_args': ['//div[@data-pl="product-reviewer"]//strong/text()']
                    },
                    {'_fn': 'amount_from_string'}
                ]
            },
            'Reviews count': {
                '_fns': [
                    {
                        '_fn': 'xpath_one',
                        '_args': ['//a[@href="#nav-review"]/text()']
                    },
                    {'_fn': 'amount_from_string'}
                ]
            },
            'Delivery': {
                '_fns': [{
                    '_fn': 'xpath_one',
                    '_args': ['//div[contains(@class, "dynamic-shipping")]//strong/text()']
                }]
            },
            'Specifications': {
                '_fns': [{
                    '_fn': 'xpath',
                    '_args': ['//ul[contains(@class, "specification--list")]//li/div']
                }],
                '_items': {
                    'Title': {
                        '_fns': [{
                            '_fn': 'xpath_one',
                            '_args': ['.//div[contains(@class, "title")]//text()']
                        }]
                    },
                    'Description': {
                        '_fns': [{
                            '_fn': 'xpath_one',
                            '_args': ['.//div[contains(@class, "desc")]//text()']
                        }]
                    }
                }
            }
        }
    }

    print(f'About to work on {len(product_urls)} products')
    
    data = []

    for url in product_urls:
        payload['url'] = 'http://' + url
        response = requests.post('https://realtime.oxylabs.io/v1/queries', auth=API_credentials, json=payload)


        # Debugging response content
        print("Response status code:", response.status_code)
        if response.status_code != 200:
            print("Response content:", response.content)
            response.raise_for_status()

        response_page = response.json()

        result = response_page['results'][0]['content']
        
        if result['Specifications']:
            specifications = []
            for spec in result['Specifications']:
                string = f'{spec["Title"]}: {spec["Description"]}'
                specifications.append(string)
            result['Specifications'] = ';\n '.join(specifications)
        else:
            result['Specifications'] = None

        result['URL'] = url
        result['id'] = url.split('/')[-1].split('.')[0]
        data.append(result)
        print(f'product {url.split('/')[-1].split('.')[0]} information appended')

    data_path = f"/opt/airflow/data/products_info/product_info.json"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    with open(data_path, "w") as rfile:
        json.dump(data, rfile, indent=4)

    print('Done with gathering product information')

    # DATAFRAME
    df = pd.DataFrame(data)
    # Apply the function to each row
    df = df.apply(clean_item, axis=1)
    # Define file path
    file_path = f'/opt/airflow/data/final/products_info.csv'
    # Save DataFrame to CSV
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False, encoding='utf-8')

def get_productReview(review_url):
    # Send a request to the API.
    response = requests.request(
        'POST',
        'https://realtime.oxylabs.io/v1/queries',
        auth=API_credentials,
        json={
            'source': 'universal_ecommerce',
            'url': review_url, # Pass the processed reviews URL.
            'geo_location': 'United States',
            'user_agent_type': 'desktop'
        }
    )

    results = response.json()['results'][0]['content']
    data = json.loads(results)



    # Parse each review and append the results to a list.
    parsed_reviews = []
    for review in data['data']['evaViewList']:
        parsed_review = {
            'productId': review_url.split('=')[1].split('&')[0],
            'Rating': review.get('buyerEval', ''),
            'Date': review.get('evalDate', ''),
            'Feedback_translated': review.get('buyerTranslationFeedback', ''),
            'Feedback': review.get('buyerFeedback', ''),
            review.get('reviewLabel1', ''): review.get('reviewLabelValue1', ''),
            review.get('reviewLabel2', ''): review.get('reviewLabelValue2', ''),
            review.get('reviewLabel3', ''): review.get('reviewLabelValue3', ''),
            'Name': review.get('buyerName', ''),
            'Country': review.get('buyerCountry', ''),
            'Upvotes': review.get('upVoteCount', ''),
            'Downvotes': review.get('downVoteCount', '')
        }
        parsed_reviews.append(parsed_review)

    parsed_reviews_path = f'/opt/airflow/data/parsed_reviews/{review_url.split('=')[1].split('&')[0]}_parsed_reviews.json'
    os.makedirs(os.path.dirname(parsed_reviews_path), exist_ok=True)
    with open(parsed_reviews_path, "w") as rfile:
        json.dump(parsed_reviews, rfile, indent=4)

    
    # Create DataFrame from parsed_reviews
    df = pd.DataFrame(parsed_reviews)
    # Define file path
    file_path = f'/opt/airflow/data/reviews/{review_url.split("=")[1].split("&")[0]}_reviews.csv'
    # Save DataFrame to CSV
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False, encoding='utf-8')

    return parsed_reviews

def get_product_reviews(**kwargs):
   # Retrieve the product URLs from XCom
    ti = kwargs['ti']
    product_urls = ti.xcom_pull(task_ids='extract_top_selling_products')
   
    all_parsed_reviews = []
    for url in product_urls:
        # Specify the maximum number of reviews to extract.
        max_reviews = 100   
        # Get the product ID from the URL
        product_id = re.match(r'.*/(\d+)\.html$', url).group(1)
        review_url = f'https://feedback.aliexpress.com/pc/searchEvaluation.do?productId={product_id}&lang=en_US&country=US&pageSize={max_reviews}&filter=all&sort=complex_default'
        
        reviews_parsed = get_productReview(review_url)
        all_parsed_reviews.extend(reviews_parsed)
               
        print(f'Done with {product_id}')

    all_parsed_reviews_path = f'/opt/airflow/data/all_parsed_reviews/all_parsed_reviews.json'
    os.makedirs(os.path.dirname(all_parsed_reviews_path), exist_ok=True)
    with open(all_parsed_reviews_path, "w") as rfile:
        json.dump(all_parsed_reviews, rfile, indent=4)
    
    # Create DataFrame from parsed_reviews
    df = pd.DataFrame(all_parsed_reviews)
    # Define file path
    file_path = f'/opt/airflow/data/final/all_parsed_reviews.csv'
    # Save DataFrame to CSV
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False, encoding='utf-8')


def upload_folder_to_s3(bucket_name, s3_key_prefix, local_folder, **kwargs):
    s3 = S3Hook(aws_conn_id='aws_default')

    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = os.path.join(s3_key_prefix, os.path.relpath(local_path, local_folder))
            s3.load_file(filename=local_path, key=s3_key, bucket_name=bucket_name, replace=True)
            print(f'Uploaded {local_path} to s3://{bucket_name}/{s3_key}')



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 28, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# DAG for extracting top-selling products
dag_ali_crawler = DAG (
    'aliexpress_crawler',
    default_args=default_args,
    description='A DAG to extract top-selling products and its respective reviews from AliExpress',
    schedule_interval=timedelta(days=1),
)



# Task for extracting top-selling products
task_extract_top_selling_products = PythonOperator(
    task_id='extract_top_selling_products',
    python_callable=get_top_selling_products,
    provide_context=True,
    dag=dag_ali_crawler,
)

# Task for extracting product information
task_extract_product_information = PythonOperator(
    task_id='extract_product_information',
    python_callable=get_product_information,
    provide_context=True,
    dag=dag_ali_crawler,
)

# Task for extracting product reviews
task_extract_product_reviews = PythonOperator(
    task_id='extract_product_reviews',
    python_callable=get_product_reviews,
    provide_context=True,
    dag=dag_ali_crawler,
)


task_upload_folder_to_s3 = PythonOperator(
    task_id='upload_folder_to_s3',
    python_callable=upload_folder_to_s3,
    op_kwargs={
        'bucket_name': 'bucket-ali-crawler',
        's3_key_prefix': s3_prefix,
        'local_folder': '/opt/airflow/data/',
    },
    provide_context=True,
    dag=dag_ali_crawler,
)

task_extract_top_selling_products >> task_extract_product_information >> task_extract_product_reviews >> task_upload_folder_to_s3

