import requests
import re
import time
from utils.lista3 import lista
from bs4 import BeautifulSoup
from tqdm import tqdm

nova_lista = []

# lista = ['15ZINE THEME', 'teste']

for texto in lista:
    # Extrai os números da string
    # numbers = re.findall(r"[^a-zA-Z\-\' ']", texto) if re.findall(r"[^a-zA-Z\-\' ']", texto) else ""
    # Remove os números da string  
    string_wit_numbers = re.sub(r"[a-zA-Z\-\' ']", '', texto)
    string_without_numbers = re.sub(r"[^a-zA-Z\-\' ']", '', texto)
    result = string_without_numbers.replace(" ", "-")
    result = '-'.join(word.capitalize() for i, word in enumerate(result.split(sep='-')))
    result = f'{string_wit_numbers}{result}'
    nova_lista.append((texto,result))

# lista = []
pbar = tqdm(nova_lista)
arquivo = open('lista3.txt', 'w')

for texto,link in pbar:
    url = f"https://trends.builtwith.com/framework/{link}"
    response = requests.get(url)

    if response.status_code == 200:        
        soup = BeautifulSoup(response.text, "html.parser")
        if "WordPress Theme" in soup.text:
            # lista.append((texto,'WordPress Theme'))
            arquivo.write(f'{texto},WordPress Theme\n')            
            
        time.sleep(0.5)
    pbar.set_description('Progresso..')
        

# for texto1, texto2 in lista:
#     with open('lista.txt', 'w') as f:
#         f.write(f'{texto1},{texto2}')
