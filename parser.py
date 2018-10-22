import re
import idna
import asyncio

import tldextract
from tld import get_tld
from tld.exceptions import TldBadUrl
from urllib.parse import urljoin, urldefrag, unquote
from lxml import html, etree


import settings
from crawler import WebSpider


class NetCrawler(WebSpider):

    async def get_parsed_content(self, url_data):
        url_to_parse = await self.get_url(url_data)
        page_tld = tldextract.extract(url_to_parse)[2]
        # tld = get_tld(url_to_parse, as_object=True)

        ids = url_data['domain_id'] if url_data['table'] == 'domains' else url_data['page_id']

        self.data = {'db': page_tld, 'table': url_data['table'], 'ids': ids}

        answer = await self.get_html_from_url(url_to_parse)
        if answer['http_status'] < 100:
            self.data.update(answer)
            await self.update_data()
            return

        self.data.update({'http_status': answer['http_status']})

        document = answer['document']
        if self.data['table'] == 'domains':
            len_content, title = await self.extract_title(document)
            self.data.update({'len_content': len_content, 'title': title[:250], 'http_status': answer['http_status']})

        await self.update_data()

        if self.test:
            document = self.test

        domains, pages, backlinks, self.redirects = await self.get_urls(self, document, page_tld,
                                                                        answer['host'], answer['real_url'])

        if domains:
            domains = await self.to_idna(domains)

            # convert to insert to DB
            self.domains = str(list({domain} for domain in domains))[1:-1].replace('{', '(').replace('}', ')')
            await self.insert_domains()

        if pages and url_data.get('depth', 0) <= url_data['max_depth']: # if depth == max we don't gather internal pages
            pages = await self.remove_trash(pages)
            pages = await self.remove_filelinks(pages)

            depth = 1 if url_data['table'] == 'domains' else url_data['depth'] + 1

            # convert to insert to DB
            self.pages = str(tuple((url_data['domain_id'], url_data['max_depth'], depth, page_url) for page_url in pages))[1:-1]
            await self.insert_pages()

        if backlinks:
            page_id = url_data.get('page_id', 0)
            self.backlinks = str(tuple((url_data['domain_id'], page_id, backlink[0], backlink[1], backlink[2]) for backlink in backlinks))[1:-1]
            if len(backlinks) == 1:
                self.backlinks = self.backlinks[:-1] # Убираем лишнюю запятую: '(341, 0, 'koerteklubi.ee', 'span', True),'
            await self.insert_backlinks()

        if answer.get('redirects', None):
            self.redirect_list = await self.get_redirects(answer['redirects'], url_data['domain'])
            await self.insert_redirects(url_data.get('domain_id', 0), answer['redirects'])


    @staticmethod
    async def get_urls(self, document, page_tld, host, page_domain):
        urls = set()
        domains = set()
        backlinks = set()
        redirects = set()
        # capture = re.compile(f".{tld}")
        dom = html.fromstring(document)
        for link in set(dom.xpath('//a')):
            # reset to default
            href = url= link_url = link_tld = internal_link = ''

            try:
                href = link.xpath('./@href')[0].lower()
            except IndexError:
                continue

            url = unquote(urldefrag(href)[0])

            url = url.split('@')[-1:][0]  # remove mailto part
            if url == page_domain: # skip this option
                continue

            if url:

                link_url = tldextract.extract(url)
                link_tld = link_url[2]
                # try:
                #     res = tldextract.extract(url)[2]
                # except TldBadUrl:
                if not link_tld:
                    # url has not tld - it's URI, then create full URL
                    url = url[1:] if url.startswith('./') else url
                    url = url[1:] if url.startswith('//') else url
                    delimiter = '' if url.startswith('/') else '/'
                    internal_link = f"{page_domain}{delimiter}{url}"

                if link_tld == page_tld:
                    # if uri exist:
                    if len(url.split(f".{link_tld}")[1]) > 1:
                        urls.add(url)
                    if host not in url and url not in host:
                        anchor = await self.get_anchor(link)
                        dofollow = await self.get_rel(link)
                        backlinks.add((url, anchor, dofollow))
                        domain = '.'.join(link_url[1:3])
                        if len(domain) < 64: # check for maximum domain length
                            domains.add(domain)
                elif internal_link:
                    urls.add(internal_link)

        return domains, urls, backlinks, redirects

    @staticmethod
    async def get_url(url_data):
        if url_data.get('table', None) == 'domains':
            return f"http://{url_data['domain']}"
        return url_data['page_url']

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
        try:
            html_page = html.fromstring(page)
        except ValueError as exc:
            print(f"Error on the page: {page}")
            raise ValueError(exc)

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

    @staticmethod
    async def get_anchor(link):
        anchor = img = ''

        for element in link.iter():
            # print(element.tag, '-', element.text)
            img = 'image' if element.tag == 'img' else ''
            if element.text is not None:
                text = element.text.replace('\n', '').replace('\t', '').replace('\r', '').strip()
                # text = text if 'mailto' not in text else 'mailto'
                anchor = text if text else img

                if anchor:
                    return anchor[:199]

        return


    @staticmethod
    async def get_rel(link):
        dofollow = True
        try:
            rel = link.xpath('./@rel')[0]
            if 'nofollow' in rel:
                dofollow = False
        except IndexError:
            pass

        return dofollow

    @staticmethod
    async def to_idna(domains):
        idna_domains = list()
        for domain in domains:
            try:
                idna_domains.append(idna.decode(domain))
            except:
                idna_domains.append(domain)

        return idna_domains

    @staticmethod
    async def get_redirects(redirects, domain_url):
        redirect_list = list()

        for redirect in redirects:
            str_redirect = str(redirect)
            try:
                redirect_code = str_redirect.split('[')[1].split(' ')[0]
            except:
                redirect_code = ''

            try:
                redirect_to = str_redirect.split('Location')[1].split("'")[2]
            except:
                redirect_to = ''

            if redirect_code or redirect_to:
                if domain_url not in redirect_to:
                    redirect_list.append((redirect_code, redirect_to))

        return redirect_list


if __name__ == '__main__':
    pass
