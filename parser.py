import re
import idna
import asyncio
from urllib.parse import unquote

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
        domain_url = '.'.join(tldextract.extract(url_to_parse)[1:3])
        page_tld = tldextract.extract(url_to_parse)[2]
        # tld = get_tld(url_to_parse, as_object=True)

        ids = url_data['domain_id'] if url_data['table'] == 'domains' else url_data['page_id']

        self.data = {'db': page_tld, 'table': url_data['table'], 'ids': ids}

        if page_tld not in settings.TLDS:
            return

        answer = await self.get_html_from_url(url_to_parse)
        if answer.get('http_status', 0) < 100:
            self.data.update(answer)
            await self.update_data()
            return

        self.data.update({'http_status': answer['http_status']})

        html_page = self.get_html(answer.get('document', ''))

        if self.data['table'] == 'domains':
            # print(f"len(document): {len(document)}")
            if html_page is not None:
                len_content, title = await self.extract_title(html_page)
            else:
                len_content = 0
                title = 'No content'
            self.data.update({'len_content': len_content, 'title': title[:250], 'http_status': answer['http_status']})

        await self.update_data()

        if html_page is not None:
            domains, pages, backlinks, self.redirects = await self.get_urls(self, html_page, page_tld,
                                                                            answer['host'], answer['real_url'])

            if domains:
                domains = await self.to_idna(domains)

                # convert to insert to DB
                self.domains = str(list({domain} for domain in domains))[1:-1].replace('{', '(').replace('}', ')')
                await self.insert_domains()

            if pages and url_data.get('depth', 0) <= url_data['max_depth']: # if depth == max we don't gather internal pages
                pages = list(page for page in pages if len(page) < 255)
                pages = await self.remove_trash(pages)
                pages = await self.remove_filelinks(pages)

                if pages:

                    depth = 1 if url_data['table'] == 'domains' else url_data['depth'] + 1

                    # convert to insert to DB
                    self.pages = str(tuple((url_data['domain_id'], url_data['max_depth'], url_data['ip_id'], depth, page_url) for page_url in pages))[1:-1]
                    if len(pages) == 1:
                        self.pages = self.pages[:-1]
                    await self.insert_pages()

            if backlinks:
                page_id = url_data.get('page_id', 0)
                self.backlinks = str(tuple((url_data['domain_id'], page_id, backlink[0][:240], backlink[1].replace("'", ' ')[:190], backlink[2]) for backlink in backlinks))[1:-1]
                if len(backlinks) == 1:
                    self.backlinks = self.backlinks[:-1] # Убираем лишнюю запятую: '(341, 0, 'koerteklubi.ee', 'span', True),'
                await self.insert_backlinks()

            if answer.get('redirects', None):
                self.redirect_list = await self.get_redirects(answer['redirects'], domain_url)
                await self.insert_redirects(url_data.get('domain_id', 0), answer['redirects'])

    def get_html(self, document):
        html_page = None

        if not len(document):
            return

        document = self.remove_xml_declaration(document)
        try:
            html_page = html.fromstring(document)
        except etree.ParserError as exc:
            if str(exc) == 'Document is empty':
                return
            print(f"ParserError on the page: {document}")
            raise ValueError(exc)
        except ValueError as exc:
            print(f"ValueError on the page: {document}")
            raise ValueError(exc)

        return html_page


    @staticmethod
    async def get_urls(self, html_page, page_tld, host, page_domain):
        urls = set()
        domains = set()
        backlinks = set()
        redirects = set()

        for link in set(html_page.xpath('//a')):
            if tldextract.extract(page_domain)[2] != page_tld:
                continue

            # reset to default
            href = url= link_url = link_tld = internal_link = ''

            try:
                href = link.xpath('./@href')[0].lower()
            except IndexError:
                continue

            url = unquote(urldefrag(href)[0]).replace("'", "")

            url = url.split('@')[-1:][0]  # remove mailto part

            if url == page_domain: # skip this option
                continue

            if len(url) > len(page_tld) + 1 :

                link_url = tldextract.extract(url)
                if link_url[1] in ['mailto', 'google']:
                    continue

                link_tld = link_url[2]

                if not link_tld:
                    # url has not tld - it's URI, then create full URL
                    url = url[1:] if url.startswith('./') else url
                    url = url[1:] if url.startswith('//') else url
                    delimiter = '' if url.startswith('/') else '/'
                    internal_link = f"{page_domain}{delimiter}{url}"

                if link_tld == page_tld:
                    # if uri exist:
                    urls.add(url)
                    if host not in url and url not in host:
                        anchor = await self.get_anchor(link)
                        dofollow = await self.get_rel(link)
                        backlinks.add((url[:249], anchor, dofollow))
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

        if url_data['page_url'].startswith('//'):
            return f"http:{url_data['page_url']}"

        if not url_data['page_url'].startswith('http'):
            return f"http://{url_data['page_url']}"

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
    async def extract_title(html_page):
        """Extract title and len(html) from html"""

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

        return ''

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

    @staticmethod
    def remove_xml_declaration(document):
        if document.startswith('<?xml'):
            end_declaration = document.find('?>')
            return document[end_declaration + 2:]
        return document


if __name__ == '__main__':
    pass
