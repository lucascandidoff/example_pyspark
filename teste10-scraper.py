import requests
import re
import time
from utils.lista import lista
from bs4 import BeautifulSoup
from tqdm import tqdm
from lxml import etree

# lista = ['NGAGE LIVE']  

pbar = tqdm(lista)
arquivo = open('lista.txt', 'w')

for tech in pbar:  
    # string_wit_numbers = re.sub(r"[a-zA-Z\-\' ']", '', texto)
    # string_without_numbers = re.sub(r"[^a-zA-Z\-\' ']", '', texto)
    # result = string_without_numbers.replace(" ", "-")
    # result = '-'.join(word.capitalize() for i, word in enumerate(result.split(sep='-')))
    # result = f'{string_wit_numbers}{result}'

    try:
        texto = tech.replace(' ','%20')
        response = requests.get(f"https://autocomplete.builtwith.com/ac.asmx?t=y&i=BR&q={texto}")           
        if response.status_code == 200: 
            if response.text == '[]':
                # arquivo.write(f'{tech},null\n')      
                arquivo.write(f'null|null\n')      
            else:

                soup = BeautifulSoup(response.content, 'html.parser')
                texto = soup.currentTag.text.replace('null','None')
                texto = eval(texto)[0]['html']
                link = eval(texto.split('href=')[1].split('class')[0].replace('//','https://'))
               
                texto = response.text.replace('null','None')
                html =  eval(texto)[0]['html']
                html = BeautifulSoup(html, "html.parser")
                tecnologia = html.text.split('Technology Result:')[1].split('\n')[0]
                # link = eval(texto)[len(eval(texto))-1]['text']
                if ' · ' in tecnologia:
                    tecnologia = tecnologia.split(' · ')[len(tecnologia.split(' · '))-1]
                tecnologia = tecnologia.strip().upper()
                # arquivo.write(f'{tech},{tecnologia}\n')

                response = requests.get(link)   
                link_tec = 'null'        
                if response.status_code == 200: 
                    soup = BeautifulSoup(response.content, 'html.parser')
                    link_tec = soup.find_all("div",{"class":"col-9 col-md-10"})[0].contents[3].next.contents[0]

                # response = requests.get(link_tec,headers={'User-Agent': 'Mozilla/5.0'})
                # if response.status_code == 200: 
                #     soup = BeautifulSoup(response.content, 'html.parser')

                arquivo.write(f'{tecnologia}|{link_tec}\n')
        else:
            # arquivo.write(f'{tech},null\n')    
            arquivo.write(f'null|null\n')    
        # time.sleep(0.3)      
    except Exception as e: 
        # arquivo.write(f'{tech},null\n')    
        arquivo.write(f'null|null\n')    
        print(f"Erro: {e}")
        print(f"Tech: {tech}")
