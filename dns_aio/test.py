from swarm_resolver import SwarmResolver
import requests
import pprint

if __name__ == '__main__':
    domains = [
        "delfi.ee",
        "elaenud.ee",
        "neti.ee",
        "dklex.ee",
        "rethryt.lt",
        "rethryt.ee",
        "rethryt.lv",
        "rethryt.fi",
    ]

    #Bigger testing speed
    #d = requests.get("https://raw.githubusercontent.com/opendns/public-domain-lists/master/opendns-random-domains.txt").text
    #for domain in d.split("\n"):
    #    domains.append(domain)

    swarm = SwarmResolver(qtype="A", num_workers=10)

    ip_domains = swarm.resolve_list(domains)

    # pprint.pprint(ip_domains)

    for domain, data in ip_domains.items():
        print(f"domain_row: {domain} {data[0][0]}")
        print(f"domain data: {data}")
        print(f"domain data len: {len(data)}")
