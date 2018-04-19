import json
import xmltodict
import base64
from scrapy import Spider
from scrapy import Request
from discourse.extract_rtf import striprtf
from discourse.load import load_urls

class DiscourseSpider(Spider):
    name = "discourses"

    def start_requests(self):
        urls = load_urls()

        for url in urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        discourses_dict = xmltodict.parse(response.body)
        text = striprtf(base64.b64decode(discourses_dict['sessao']['discursoRTFBase64'])).replace('\t', '').replace('\n', '').replace('\x00', '')
        discourses = {'speaker': discourses_dict['sessao']['nome'],
                      'party': discourses_dict['sessao']['partido'],
                      'date': discourses_dict['sessao']['horaInicioDiscurso'],
                      'discourse': text,
                      'url': response.url}

        with open('../../../data/file.json', 'a') as f:
            f.write(json.dumps(discourses, ensure_ascii=False))
            f.write('\n')
