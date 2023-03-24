import requests
import re
import time
from utils.lista import lista
from bs4 import BeautifulSoup
from tqdm import tqdm

# lista = ['ADOBE SYSTEMS']  

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
                arquivo.write(f'null,null\n')      
            else:
                texto = response.text.replace('null','None')
                html =  eval(texto)[0]['html']
                html = BeautifulSoup(html, "html.parser")
                tecnologia = html.text.split('Technology Result:')[1].split('\n')[0]
                link = eval(texto)[len(eval(texto))-1]['text']
                if ' · ' in tecnologia:
                    tecnologia = tecnologia.split(' · ')[len(tecnologia.split(' · '))-1]
                tecnologia = tecnologia.strip().upper()
                # arquivo.write(f'{tech},{tecnologia}\n')
                arquivo.write(f'{tecnologia},{link}\n')
        else:
            # arquivo.write(f'{tech},null\n')    
            arquivo.write(f'null,null\n')    
        # time.sleep(0.3)      
    except Exception as e: 
        # arquivo.write(f'{tech},null\n')    
        arquivo.write(f'null,null\n')    
        print(f"Erro: {e}")
        print(f"Tech: {tech}")
