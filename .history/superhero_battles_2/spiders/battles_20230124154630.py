import scrapy
from urllib.parse import urljoin

import json
import pandas as pd
filename = r'C:\Users\Baraa\Documents\Python\Testing\Superheroes_test\superhero_battles_2\superhero_battles_2\battles.json'
with open(filename) as f:
    data = json.load(f)
filename_2 = r'C:\Users\Baraa\Documents\Python\Testing\Superheroes_test\superhero_battles_2\superhero_battles_2\battles_2.jsonlines'
data_2 = []
with open(filename_2) as f:
    for line in f:
        data_2.append(json.loads(line))  
filename_3 = r'C:\Users\Baraa\Documents\Python\Testing\Superheroes_test\superhero_battles_2\battles_3.jsonlines'       
data_3 = []
with open(filename_3) as f:
    for line in f:
        data_3.append(json.loads(line))      
urls = pd.concat([pd.DataFrame(data), pd.DataFrame(data_2), pd.DataFrame(data_3) ]).Battle_url

class BattlesSpider(scrapy.Spider):
    name = 'battles'
    allowed_domains = ['www.superherodb.com']
    start_urls = [f'https://www.superherodb.com/users/?page_nr={i}' for i in range(1,3)]

    def parse(self, response):
        urls = response.css('div[class="column col-12"]> ul> li > a::attr(href)').getall()[:-4]
        urls = [urljoin(response.url, i) for i in urls]
        urls = [i+'battles/' for i in urls] 
        for url in urls:
            yield scrapy.Request(url, self.parse_user)
            
    def parse_user(self, response):
        #This function is to know the number of pages in the battle section for each user          
        num_of_pages = len(response.css('li[class="page-item"]'))         
        if num_of_pages == 0:
            yield scrapy.Request(response.url, self.parse_battles)
            return None
        elif num_of_pages == 1: #for the range function to go to only page 2
            num_of_pages = 3
        else:
            for page_num in range(1, num_of_pages):
                url = response.url + f'?page_nr={page_num}'            
                yield scrapy.Request(url, self.parse_battles)
                
    def parse_battles(self, response):
        rows =  response.css('table[class="table table-battlelist"]>tbody>tr')
        if rows:
            rows = rows[1:]
            for row in rows:
                battle_text =  'vs'.join (row.css('td::text').getall()[:2])
                if any(i for i in battle_text.lower().split(' ') if i in ("team", '&')): # Do not take urls that have multiple superheroes
                    battle_url =   urljoin('https://www.superherodb.com', row.css('td>a::attr(href)').get())                  
                    if battle_url not in urls:
                        yield scrapy.Request(battle_url, self.parse_battle_result)
    
    def parse_battle_result(self, response):
        superheroes_info = response.css('div[class="column col-6"]')
        
        if len(superheroes_info) == 2: #If only two superheroes exist, then parse
            Character_1 = superheroes_info[0].css('div>div> div> div> div> a::text').get(default = "")
            Name_1 = superheroes_info[0].css(
                    'div>div> div> div> div>a>span[class = "suffix level-1"]::text').get(default="")
            Universe_1 = superheroes_info[0].css(
                        'div>div> div> div> div>a>span[class = "suffix level-2"]::text').get(default="")
            Character_2 = superheroes_info[1].css('div>div> div> div> div>a::text').get(default = "")
            Name_2 = superheroes_info[1].css(
                    'div>div> div> div> div>a>span[class = "suffix level-1"]::text').get(default="")
            Universe_2 = superheroes_info[1].css(
                        'div>div> div> div> div>a>span[class = "suffix level-2"]::text').get(default="")
            
            Battle_title = Character_1 +' vs ' + Character_2
            results = response.css('div[class="battle3-team-header"] > div::text').getall()
            #If you wish you can add the draw result, but it can be computed by subtracting superhero_1_result and superher_2_result
            #so it's not necessary to scrape
            #draw_result = response.css('div[class="battle-options"] > div>   div.battle3-team-result::text').get(default = "")
            results = {"Superhero_1_win_result": results[0], "Superhero_2_win_result": results[1]}
            battle_summary = {"Battle_title": Battle_title, "Battle_url": response.url,
                            "Superhero_1": {"Character": Character_1, "Name": Name_1, "Universe": Universe_1}, 
                            "Superhero_2": {"Character": Character_2, "Name": Name_2, "Universe": Universe_2},
                            "Result": results}
            yield battle_summary
            
            
            
        
