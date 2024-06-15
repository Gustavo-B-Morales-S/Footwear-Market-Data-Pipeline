import scrapy
from typing import List, AnyStr

class MercadolivreSpider(scrapy.Spider):
    name = "mercadolivre"
    allowed_domains = ["lista.mercadolivre.com.br"]
    start_urls = ["https://lista.mercadolivre.com.br/tenis-corrida-masculino"]

    def parse(self, response):
        current_page = response.css('li.andes-pagination__button.andes-pagination__button--current a::text').get()
        max_page_count = '10'
        
        product_html_element = 'div.ui-search-result__content'
        brand_html_element = 'span.ui-search-item__brand-discoverability.ui-search-item__group__element::text'
        name_html_element = 'h2.ui-search-item__title::text'
        price_html_element = 'span.andes-money-amount__fraction::text'
        price_cents_html_element = 'span.andes-money-amount__cents::text'
        reviews_rating_number_html_element = 'span.ui-search-reviews__rating-number::text'
        reviews_amount_html_element = 'span.ui-search-reviews__amount::text'
        
        products: List[AnyStr] = response.css(product_html_element)
        
        for product in products:
            print(current_page, '')
            prices: List[AnyStr] = product.css(price_html_element).getall()
            cents: List[AnyStr] = product.css(price_cents_html_element).getall()
            
             
            yield {
                'brand': product.css(brand_html_element).get(),
                'name': product.css(name_html_element).get(),
                'old_price_reais': prices[0] if len(prices) > 0 else None,
                'old_price_centavos': cents[0] if len(cents) > 0 else None,
                'new_price_reais': prices[1] if len(prices) > 1 else None,
                'new_price_centavos': cents[1] if len(cents) > 1 else None,
                'reviews_rating_number': response.css(reviews_rating_number_html_element).get(),
                'reviews_amount': response.css(reviews_amount_html_element).get(),
            } 
        # Esta é a quantidade de paginas pela qual vou percorrer!
        if current_page != max_page_count:
            # Next Page URL
            next_page_url: str = response.css('li.andes-pagination__button.andes-pagination__button--next a::attr(href)').get()
            yield scrapy.Request(url=next_page_url, callback=self.parse)
