import requests
import re
import time
from utils.lista import lista
from bs4 import BeautifulSoup
from tqdm import tqdm

# nova_lista = []

# lista = ['AUTO REPAIR AND CAR MECHANIC']

pbar = tqdm(lista)
arquivo = open('lista_completa.txt', 'w')

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
                arquivo.write(f'{tech},null\n')      
            else:
                texto = response.text.replace('null','None')
                texto =  eval(texto)[0]['html']
                html = BeautifulSoup(texto, "html.parser")
                tecnologia = html.text.split('Technology Result:')[1].split('\n')[0]
                if ' · ' in tecnologia:
                    tecnologia = tecnologia.split(' · ')[1]
                tecnologia = tecnologia.strip().upper()
                arquivo.write(f'{tech},{tecnologia}\n')
        else:
            arquivo.write(f'{tech},null\n')    
        # time.sleep(0.3)      
    except Exception as e: 
        arquivo.write(f'{tech},null\n')    
        print(f"Erro: {e}")
        print(f"Tech: {tech}")
    # nova_lista.append((texto,result))

# lista = []


exit()

pbar = tqdm(nova_lista)
arquivo = open('lista_teste.txt', 'w')

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
