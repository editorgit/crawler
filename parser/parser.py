import asyncio
import logging

from _datetime import datetime
import tldextract
from lxml import html, etree
from urllib.parse import urldefrag, unquote

import settings

from parser.domains import processing_domains
from parser.pages import processing_pages
from parser.backlinks import processing_backlinks
from parser.redirects import processing_redirects


logging.basicConfig(filename=settings.LOG_PARSER, level=settings.LOGGER_LEVEL, format=settings.FORMAT)


class PageParser:

    html_page: str = ""
    log = logging.getLogger()

    def __init__(self):
        self.db_conn_dict = dict()
        self.db_data = dict()

    async def set_db_conn_dict(self, db_conn_dict):
        self.db_conn_dict = db_conn_dict

    async def set_db_data(self, url_data, page_tld):
        ids = url_data['domain_id'] if url_data['table'] == 'domains' else url_data['page_id']
        self.db_data = {'db': str(page_tld), 'table': url_data['table'], 'ids': ids}

    async def parse_content(self, db_conn_dict, url_data, answer, domain_url, page_tld):
        await self.set_db_conn_dict(db_conn_dict)
        await self.set_db_data(url_data, page_tld)

        # insert answer with errors
        if answer.get('http_status', 0) < 100:
            self.db_data.update(answer)
            await self.update_source()
            return

        # update data for source
        await self.update_source_data(answer)

        # convert to eetree element
        self.html_page = await self.get_html(answer.get('document', ''))

        # parse page
        if self.html_page is not None:
            await self.parse_page(url_data, answer, domain_url)

    async def update_source_data(self, answer):
        if self.db_data['table'] == 'domains':
            len_content = 0
            title = 'No content'

            if self.html_page is not None:
                len_content, title = await self.extract_title()

            self.db_data.update({'len_content': len_content, 'title': title[:250], 'http_status': answer['http_status']})

        # update domain or url which was source for this data
        await self.update_source()

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
            self.log.info(f"ParserError on the page: {document}")
            raise ValueError(exc)
        except ValueError as exc:
            self.log.info(f"ValueError on the page: {document}")
            raise ValueError(exc)

        return html_page

    async def parse_page(self, url_data, answer, domain_url):
        domains, pages, backlinks, redirects = await self.get_data_from_page(
            self, self.db_data['db'],
            answer['host'],
            answer['real_url']
        )

        # insert_domains
        if domains:
            sql = await processing_domains(domains)
            await self.db_conn_dict[self.db_data['db']].execute(sql)

        # insert_pages
        # if depth == max we don't gather internal pages
        if pages and url_data.get('depth', 0) < url_data['max_depth']:
            sql = await processing_pages(pages, url_data)

            if sql:
                await self.db_conn_dict[self.db_data['db']].execute(sql)

        # insert_backlinks
        if backlinks:
            sql = await processing_backlinks(backlinks, url_data)
            await self.db_conn_dict[self.db_data['db']].execute(sql)

        # insert_redirects
        redirects = answer.get('redirects', None)
        if redirects:
            sql = await processing_redirects(redirects, domain_url, url_data, db_data)
            await self.db_conn_dict[self.db_data['db']].execute(sql)

    @staticmethod
    async def get_data_from_page(self, page_tld, host, page_domain):
        urls = set()
        domains = set()
        backlinks = set()
        redirects = set()

        for link in set(self.html_page.xpath('//a')):
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

            if url == page_domain:  # skip this option
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

    async def extract_title(self):
        """Extract title and len(html) from html"""

        try:
            len_content = len(self.html_page.xpath("//text()"))
        except AttributeError:
            return 0, ''

        try:
            title = self.html_page.xpath("//title/text()")[0]
            title = title.replace(';', '').replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        except (AttributeError, IndexError):
            title = ''
        return len_content, title.strip()

    @staticmethod
    async def get_anchor(link):
        anchor = img = ''

        for element in link.iter():
            # self.log.info(element.tag, '-', element.text)
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

    async def update_source(self):
        sql = await self.sql_update_source()
        await self.db_conn_dict[self.db_data['db']].execute(sql)

    async def sql_update_source(self):
        # if domain or pages
        now = datetime.now()
        if self.db_data['table'] == 'domains':
            title = self.db_data['title'].replace("'", "")
            sql = "UPDATE domains " \
                  "SET http_status_code=%s, title='%s', in_job=Null, last_visit_at='%s', len_content=%s " \
                  "WHERE ids=%s" % (self.db_data['http_status'], title, now,
                                    self.db_data.get('len_content', 0), self.db_data['ids'])
        else:
            sql = f"UPDATE {self.db_data['table']} " \
                  f"SET http_status_code={self.db_data['http_status']}, in_job=Null, last_visit_at='{now}' " \
                  f"WHERE ids={self.db_data['ids']}"
        return sql
