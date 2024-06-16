import scrapy
from typing import List, AnyStr

class MercadolivreSpider(scrapy.Spider):
    name = "mercadolivre"
    allowed_domains = ["lista.mercadolivre.com.br"]
    start_urls = ["https://lista.mercadolivre.com.br/tenis-corrida-masculino"]

    def parse(self, response):
        # Setting page limit
        current_page_selector = response.css('li.andes-pagination__button.andes-pagination__button--current a::text').get()
        max_page_count = '10'
        
        # Setting selectors
        product_selector = 'div.ui-search-result__content'
        brand_selector = 'span.ui-search-item__brand-discoverability.ui-search-item__group__element::text'
        name_selector = 'h2.ui-search-item__title::text'
        price_selector = 'span.andes-money-amount__fraction::text'
        price_in_cents_selector = 'span.andes-money-amount__cents::text'
        reviews_rating_number_selector = 'span.ui-search-reviews__rating-number::text'
        reviews_amount_selector = 'span.ui-search-reviews__amount::text'
        
        products: List[AnyStr] = response.css(product_selector)
        
        for product in products:
            prices: List[AnyStr] = product.css(price_selector).getall()
            cents: List[AnyStr] = product.css(price_in_cents_selector).getall()
            
             
            yield {
                'brand': product.css(brand_selector).get(),
                'name': product.css(name_selector).get(),
                'old_price_in_reais': prices[0] if len(prices) > 0 else None,
                'old_price_in_cents': cents[0] if len(cents) > 0 else None,
                'new_price_in_reais': prices[1] if len(prices) > 1 else None,
                'new_price_in_cents': cents[1] if len(cents) > 1 else None,
                'reviews_rating_number': response.css(reviews_rating_number_selector).get(),
                'reviews_amount': response.css(reviews_amount_selector).get(),
            } 

        if current_page_selector != max_page_count:
            next_page_url: str = response.css('li.andes-pagination__button.andes-pagination__button--next a::attr(href)').get()
            yield scrapy.Request(url=next_page_url, callback=self.parse)
