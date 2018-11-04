import asyncio

import settings


async def processing_backlinks(backlinks, url_data):
    page_id = url_data.get('page_id', 0)

    # convert to insert to DB
    backlinks = str(tuple(
        (url_data['domain_id'], page_id, backlink[0][:240], backlink[1].replace("'", ' ')[:180], backlink[2]) for backlink in backlinks))[1:-1]

    if backlinks[-1:] == ',':
        backlinks = backlinks[:-1]  # Убираем лишнюю запятую: '(341, 0, 'koerteklubi.ee', 'span', True),'

    sql = "INSERT INTO backlinks (donor_domain_id, donor_page_id, link_to, anchor, is_dofollow) " \
          "VALUES %s ON CONFLICT DO NOTHING" % backlinks

    return sql
