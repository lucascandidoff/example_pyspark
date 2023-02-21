import tldextract

domains = [
    '11-165-19-200.ufrnet.br',
    'google.com.br',
    'camara-e.net',
    'am.gov.br'
]

for domain in domains:
    extracted = tldextract.extract(domain)
    print(f"{extracted.domain}.{extracted.suffix}")


