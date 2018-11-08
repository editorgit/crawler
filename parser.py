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

from domains import processing_domains
from pages import processing_pages
from backlinks import processing_backlinks
from redirects import processing_redirects


class NetCrawler(WebSpider):

    async def get_parsed_content(self, url_data):
        url_to_parse = await self.get_url(url_data)
        domain_url = '.'.join(tldextract.extract(url_to_parse)[1:3])
        page_tld = tldextract.extract(url_to_parse)[2]

        ids = url_data['domain_id'] if url_data['table'] == 'domains' else url_data['page_id']

        db_data = {'db': str(page_tld), 'table': url_data['table'], 'ids': ids}

        # exclude domain or page with incorrect TLD
        if page_tld not in settings.TLDS:
            return

        # get page
        answer = await self.get_html_from_url(url_to_parse)

        # insert answer with errors
        if answer.get('http_status', 0) < 100:
            db_data.update(answer)
            await self.update_source(db_data)
            return

        db_data.update({'http_status': answer['http_status']})

        # convert to eetree element
        html_page = await self.get_html(answer.get('document', ''))

        # update dict data for domain
        if db_data['table'] == 'domains':
            if html_page is not None:
                len_content, title = await self.extract_title(html_page)
            else:
                len_content = 0
                title = 'No content'
            db_data.update({'len_content': len_content, 'title': title[:250], 'http_status': answer['http_status']})

        # update domain or url which was source for this data
        await self.update_source(db_data)

        # parse page
        if html_page is not None:
            domains, pages, backlinks, self.redirects = await self.get_urls(self, html_page, page_tld,
                                                                            answer['host'], answer['real_url'])

            # insert_domains
            if domains:
                sql = await processing_domains(domains)
                await self.db_conn_dict[db_data['db']].execute(sql)

            # insert_pages
            # if depth == max we don't gather internal pages
            if pages and url_data.get('depth', 0) <= url_data['max_depth']:
                sql = await processing_pages(pages, url_data)

                if sql:
                    await self.db_conn_dict[db_data['db']].execute(sql)

            # insert_backlinks
            if backlinks:
                sql = await processing_backlinks(backlinks, url_data)
                await self.db_conn_dict[db_data['db']].execute(sql)

            # insert_redirects
            redirects = answer.get('redirects', None)
            if redirects:
                sql = await processing_redirects(redirects, domain_url, url_data, db_data)
                await self.db_conn_dict[db_data['db']].execute(sql)

    async def get_html(self, document):
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
                    # if url not domain:
                    try:
                        if len(url.split(f'.{page_tld}')[1]) > 1:
                            urls.add(url)
                    except IndexError:
                        pass
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
    def remove_xml_declaration(document):
        if document.startswith('<?xml'):
            end_declaration = document.find('?>')
            return document[end_declaration + 2:]
        return document


if __name__ == '__main__':
    pass
