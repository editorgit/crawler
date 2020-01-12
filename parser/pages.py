import settings
from parser.domains import to_idna


async def processing_pages(pages, url_data):
    pages = list(page for page in pages if len(page) < 254)
    pages = await remove_trash(pages)
    pages = await remove_filelinks(pages)
    pages = await to_idna(pages)

    if pages:

        depth = 1 if url_data['table'] == 'domains' else url_data['depth'] + 1

        if depth > url_data['max_depth']:
            return None

        # convert to insert to DB
        pages = str(tuple(
            (url_data['domain_id'], url_data['max_depth'], url_data['ip_id'], depth, page_url) for page_url in pages))[
                     1:-1]

        if pages[-1:] == ',':
            pages = pages[:-1]

        sql = f"""INSERT INTO pages (domain_id, max_depth, ip_address, depth, page_url) VALUES {pages} ON CONFLICT DO NOTHING"""

        return sql

    return


async def remove_trash(lst):
    """Remove elem from list if elem has stop word"""
    lst = list(lst)
    for index, elem in enumerate(lst):
        for word in settings.STOP_WORDS:
            if word in elem:
                lst[index] = None
    return [elem for elem in lst if elem]


async def remove_filelinks(lst):
    """Split elem by # and return first part"""
    # print(filetypes)
    for index, elem in enumerate(lst):
        for file_type in settings.FILE_TYPES:
            if elem[-len(file_type):].lower() == file_type:
                lst[index] = None
                # print(elem, file_type, lst[index])

    return [elem for elem in lst if elem]
