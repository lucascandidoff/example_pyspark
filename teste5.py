import json

# f = open('data.json')

# data = json.load(f)

aa = ["aaa","bbb"]


notebook_params = {
    # "date": data['base_date'],
    # "incremental_type": data['incremental_type'],
    "extract": "aa"
}

# a = notebook_params['extract']

print(json.loads(notebook_params))