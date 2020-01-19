import os

import pytest

import settings

import

from parser.parser import PageParser

PATH_HTML_FILES = os.path.join(settings.BASE_DIR, 'spider/tests/html_files')


def test_parser(file_fixtures):
    html_files = [file for file in os.listdir(PATH_HTML_FILES)]

    assert len(html_files) == 0

    for html_file in html_files:
        html_page = await PageParser()._get_html(html_file)
        assert PageParser. == "Content A"
