import re
import asyncio
import string

import settings


async def processing_redirects(redirects, domain_url, url_data, data):
    redirect_list = await get_redirects(redirects, domain_url)

    domain_id = url_data.get('domain_id', 0)

    domain_pk = domain_id if domain_id else data['ids']
    page_id = data['ids'] if domain_id else 0
    redirects = re.sub('[' + string.punctuation + ']', ' ', str(redirects)[:500])

    sql = "INSERT INTO redirects (domain_id, page_id, redirect_list, redirect_raw) " \
          "VALUES (%s, %s, '%s', '%s') " \
          "ON CONFLICT DO NOTHING" % (domain_pk, page_id, str(redirect_list), redirects)

    return sql


async def get_redirects(redirects, domain_url):
    # redirect_list = list()
    redirect_list_str = ''

    for redirect in redirects:
        str_redirect = str(redirect)
        try:
            redirect_code = str_redirect.split('[')[1].split(' ')[0]
            redirect_code = ''.join([num for num in redirect_code if num.isdigit()])
        except:
            redirect_code = ''

        try:
            redirect_to = str_redirect.split('Location')[1].split("'")[2]
        except:
            redirect_to = ''

        if redirect_code or redirect_to:
            if domain_url not in redirect_to:
                redirect_list_str += f"{redirect_code}, {redirect_to};"
                # redirect_list.append((redirect_code, redirect_to))

    return redirect_list_str[:249]