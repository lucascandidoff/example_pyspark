import pandas as pd

data1 = {'id': [1, 2], 'nome': ['Lucas', 'Bernardo']}
data2 = {'id': [1, 2], 'nome': ['Candido', 'Mendes']}

df1 = pd.DataFrame(data=data1)
df2 = pd.DataFrame(data=data2)

df = df1.merge(df2, on='id', how='left')

print(df)