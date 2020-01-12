import idna
import asyncio

import settings


async def processing_domains(domains):
    domains = await to_idna(domains)
    domains = await clean_domains(domains)

    # convert to insert to DB
    domains = str(list({domain[:250]} for domain in domains))[1:-1].replace('{', '(').replace('}', ')')
    sql = f"""INSERT INTO domains (domain)
                        VALUES {domains} ON CONFLICT DO NOTHING"""
    return sql


async def to_idna(domains):
    idna_domains = list()
    for domain in domains:
        try:
            idna_domains.append(idna.decode(domain))
        except:
            idna_domains.append(domain)

    return idna_domains


async def clean_domains(domains):
    clean_domains = list()
    for domain in domains:
        clean_domain = domain.replace(';', '').replace('\n', '').replace('\r', '').replace('\t', '').replace(' ', '')
        clean_domains.append(clean_domain)

    return clean_domains
