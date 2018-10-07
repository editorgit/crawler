import re
import asyncio

from tld import get_tld
from tld.exceptions import TldBadUrl
from urllib.parse import urljoin, urldefrag, unquote
from lxml import html


import settings
from crawler import WebSpider


class NetCrawler(WebSpider):

    async def get_parsed_content(self, url_data):
        url_to_parse = await self.get_url(url_data)
        tld = get_tld(url_to_parse, as_object=True)

        self.data = {'db': tld, 'table': url_data['table'], 'ids': url_data['domain_id']}

        answer = await self.get_html_from_url(url_to_parse)
        if answer['http_status'] < 100:
            self.data.update(answer)
            await self.update_data()
            return

        document = answer['document']
        len_content, title = await self.extract_title(document)
        self.data.update({'len_content': len_content, 'title': title[:250], 'http_status': answer['http_status']})
        await self.update_data()

        self.domains, pages, self.backlinks, self.redirects = await self.get_urls(self, document, tld,
                                                                                answer['host'], answer['real_url'])

        if self.domains:
            await self.insert_domains()

        if pages:
            pages = await self.remove_trash(pages)
            pages = await self.remove_filelinks(pages)

            if url_data['table'] == 'domains':
                depth = 1
            else:
                depth = 2
            self.pages = str(tuple((url_data['domain_id'], depth, page_url) for page_url in pages))[1:-1]
            await self.insert_pages()

        if self.backlinks:
            await self.insert_backlinks()
        #
        # if self.redirects:
        #     await self.insert_redirects()


        # update record in DB: pages table
        return

    @staticmethod
    async def get_urls(self, document, tld, host, page_domain):
        urls = set()
        domains = set()
        backlinks = set()
        redirects = set()
        # capture = re.compile(f".{tld}")
        dom = html.fromstring(document)
        for link in set(dom.xpath('//a')):
            # reset to None
            res = internal_link = None

            try:
                href = link.xpath('./@href')[0].lower()
            except IndexError:
                continue

            try:
                anchor = link.xpath('./text()')[0].strip()
            except IndexError:
                img = link.xpath('./img')
                anchor = 'image' if img else ''

            try:
                rel = link.xpath('./rel')[0]
                if 'nofollow' in rel:
                    dofollow = False
            except IndexError:
                dofollow = True

            url = unquote(urldefrag(href)[0])

            url = url.split('@')[-1:][0]  # remove mailto part
            if url == page_domain: # skip this option
                continue

            if url:

                try:
                    res = get_tld(url, as_object=True)
                except TldBadUrl:
                    # url has not tld - it's URI, then create full URL
                    url = url[1:] if url.startswith('./') else url
                    delimiter = '' if url.startswith('/') else '/'
                    internal_link = f"{page_domain}{delimiter}{url}"

                if str(res) == tld:
                    # if uri exist:
                    if len(url.split(f".{tld}")[1]) > 1:
                        urls.add(url)
                    if host not in url:
                        backlinks.add((url, anchor, dofollow))
                        if len(res.fld) < 64: # check for maximum domain length
                            domains.add(res.fld)
                elif internal_link:
                    urls.add(internal_link)

        return domains, urls, backlinks, redirects

    @staticmethod
    async def get_url(url_data):
        if url_data.get('table', None) == 'domains':
            return f"http://{url_data['domain']}"
        return url_data['domain']

    @staticmethod
    async def remove_trash(lst):
        """Remove elem from list if elem has stop word"""
        lst = list(lst)
        for index, elem in enumerate(lst):
            for word in settings.STOP_WORDS:
                if word in elem:
                    lst[index] = None
        return [elem for elem in lst if elem]

    @staticmethod
    async def extract_title(page):
        """Extract title and len(html) from html"""
        html_page = html.fromstring(page)
        try:
            len_content = len(html_page.xpath("//text()"))
        except AttributeError:
            return 0, ''

        try:
            title = html_page.xpath("//title/text()")[0]
        except (AttributeError, IndexError):
            title = ''
        return len_content, title.strip()

    @staticmethod
    async def remove_filelinks(lst):
        """Split elem by # and return first part"""
        # print(filetypes)
        for index, elem in enumerate(lst):
            for file_type in settings.FILETYPES:
                if elem[-len(file_type):].lower() == file_type:
                    lst[index] = None
                    # print(elem, file_type, lst[index])

        # return [[item for item in lst if item.endswith(filetype, 5) is False] for filetype in filetypes]
        return [elem for elem in lst if elem]


if __name__ == '__main__':
    pass
