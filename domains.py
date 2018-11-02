import idna
import asyncio

import settings


async def processing_domains(domains):
    domains = await to_idna(domains)

    # convert to insert to DB
    domains = str(list({domain} for domain in domains))[1:-1].replace('{', '(').replace('}', ')')
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
