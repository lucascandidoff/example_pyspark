from unidecode import unidecode

lista = [
'aeroporto',
'alameda',
'área',
'avenida',
'campo',
'chácara',
'colônia',
'condomínio',
'conjunto',
'distrito',
'esplanada',
'estação',
'estrada',
'favela',
'fazenda',
'feira',
'jardim',
'ladeira',
'lago',
'lagoa',
'largo',
'loteamento',
'morro',
'núcleo',
'parque',
'passarela',
'pátio',
'praça',
'quadra',
'recanto',
'residencial',
'rodovia',
'rua',
'setor',
'sítio',
'travessa',
'trecho',
'trevo',
'vale',
'vereda',
'via',
'viaduto',
'viela',
'vila'
]

old = 'àáâãäèéêëìíîïòóôõöùúûüÀÁÂÃÄÈÉÊËÌÍÎÒÓÔÕÖÙÚÛÜçÇñÑ'
new = 'aaaaaeeeeiiiiooooouuuuAAAAAEEEEIIIOOOOOUUUUcCnN'

new_lista = open('lista.txt','w')

for i in lista:
    x = unidecode(i)
    new_lista.write(f"'{x.upper()}',\n")
