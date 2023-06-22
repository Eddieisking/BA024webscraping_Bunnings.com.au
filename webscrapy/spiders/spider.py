"""
Project: Web scraping for customer reviews
Author: Hào Cui
Date: 06/22/2023
"""
import json
import math
import re

import scrapy
from scrapy import Request

from webscrapy.items import WebscrapyItem


class SpiderSpider(scrapy.Spider):
    name = "spider"
    allowed_domains = ["www.bunnings.com.au", "api.bazaarvoice.com"]
    headers = {}  #

    def start_requests(self):
        # keywords = ['DeWalt', 'Black+and+Decker', 'Stanley', 'Craftsman', 'Porter-Cable', 'Bostitch', 'Irwin+Tools',
        #             'Lenox']
        # company = 'Stanley Black and Decker'

        keywords = ['dewalt']
        # from search words to generate product_urls
        for keyword in keywords:
            push_key = {'keyword': keyword}
            search_url = f'https://www.bunnings.com.au/search/products?q={keyword}&sort=BoostOrder'

            yield Request(
                url=search_url,
                callback=self.parse,
                cb_kwargs=push_key,
                # meta={'proxy':'socks5://127.0.0.1:10110'},
                # headers=self.headers
            )

    def parse(self, response, **kwargs):

        # Extract the pages of product_urls
        product_counts = response.xpath('//*[@id="main"]//div[@class="totalResults"]/p/text()')[0].extract()
        product_counts = int(re.findall(r'\d+', product_counts)[0])
        page_size = 36
        pages = math.ceil(product_counts / page_size)

        # Based on pages to build product_urls
        keyword = kwargs['keyword']
        # product_urls = [f'https://www.bunnings.com.au/search/products?q={keyword}&sort=BoostOrder&page={page}&pageSize={page_size}' \
        #                 for page in range(1, pages)]

        # test page = 1
        product_urls = [
            f'https://www.bunnings.com.au/search/products?q={keyword}&sort=BoostOrder&page={page}&pageSize={page_size}' \
            for page in range(1, 2)]

        for product_url in product_urls:
            yield Request(url=product_url, callback=self.product_parse)

    def product_parse(self, response: Request, **kwargs):

        product_list = response.xpath('//*[@id="main"]//div[@class="container-main"]//article')

        for product in product_list:
            product_href = product.xpath('.//div[@data-testid="productTileContainer"]/a/@href')[0].extract()
            product_detailed_url = f'https://www.bunnings.com.au{product_href}'
            yield Request(url=product_detailed_url, callback=self.product_detailed_parse)

    def product_detailed_parse(self, response, **kwargs):

        product_item_number = response.xpath('//*[@id="main"]//div[@class="desktopProductDetails"]//p['
                                             '@data-locator="product-item-number"]/text()')[0].extract()
        product_id = re.search(r'\d+', product_item_number).group()

        # Product reviews url
        product_reviews_url = f'https://api.bazaarvoice.com/data/reviews.json?resource=reviews&action' \
                              f'=REVIEWS_N_STATS&filter=productid%3Aeq%3A{product_id}&filter=contentlocale%3Aeq%3Aen' \
                              f'*%2Cen_AU%2Cen_AU&filter=isratingsonly%3Aeq%3Afalse&filter_reviews=contentlocale' \
                              f'%3Aeq%3Aen*%2Cen_AU%2Cen_AU&include=authors%2Cproducts&filteredstats=reviews&Stats' \
                              f'=Reviews&limit=6&offset=0&sort=helpfulness%3Adesc%2Ctotalpositivefeedbackcount' \
                              f'%3Adesc&passkey=caUZMUAJ5mm8n5r7EtVHRt5QhZFVEPcUKge0N3CDWAZFc&apiversion=5.5' \
                              f'&displaycode=10414-en_au '

        if product_reviews_url:
            yield Request(url=product_reviews_url, callback=self.review_parse)

    def review_parse(self, response: Request, **kwargs):

        datas = json.loads(response.body)

        offset_number = 0
        limit_number = 0
        total_number = 0

        offset_number = datas.get('Offset')
        limit_number = datas.get('Limit')
        total_number = datas.get('TotalResults')

        for i in range(limit_number):
            item = WebscrapyItem()
            results = datas.get('Results', [])

            try:
                item['review_id'] = results[i].get('Id', 'N/A')
                item['product_name'] = results[i].get('ProductId', 'N/A')
                item['customer_name'] = results[i].get('UserNickname', 'N/A')
                item['customer_rating'] = results[i].get('Rating', 'N/A')
                item['customer_date'] = results[i].get('SubmissionTime', 'N/A')
                item['customer_review'] = results[i].get('ReviewText', 'N/A')
                item['customer_support'] = results[i].get('TotalPositiveFeedbackCount', 'N/A')
                item['customer_disagree'] = results[i].get('TotalNegativeFeedbackCount', 'N/A')

                yield item
            except Exception as e:
                print('Exception:', e)
                break

        if (offset_number + limit_number) < total_number:
            offset_number += limit_number
            next_page = re.sub(r'limit=\d+&offset=\d+', f'limit={30}&offset={offset_number}', response.url)
            yield Request(url=next_page, callback=self.review_parse)
