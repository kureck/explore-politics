import pandas as pd
import urllib.request
import xmltodict
import base64
import aiohttp
import asyncio
import json
from extract_rtf import striprtf

# https://github.com/pmarkun/discursos-genero
# http://www.camara.gov.br/sitcamaraws/SessoesReunioes.asmx/ListarDiscursosPlenario?dataIni=23/11/2012&dataFim=23/11/2012&codigoSessao=&parteNomeParlamentar=&siglaPartido=&siglaUF=
# tem que pegar o código aqui pra poder criar a url abaixo

# sessions_ids = [{'code': '000',
#                  'discourses': [{'speaker_num': 1, 'room_num': 1, 'insertion_num': 1}]}

def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

daterange = pd.date_range('2014-10-10','2015-03-07' , freq='1M')
daterange = daterange.union([daterange[-1] + 1])
daterange = [d.strftime('%d/%m/%Y') for d in daterange]
daterange = list(chunks(daterange, 2))

sessions_url = "http://www.camara.gov.br/sitcamaraws/SessoesReunioes.asmx/ListarDiscursosPlenario?dataIni={}&dataFim={}&codigoSessao=&parteNomeParlamentar=&siglaPartido=&siglaUF="

sessions_ids = []
urls = []

discourses_url = "http://www.camara.gov.br/SitCamaraWS/SessoesReunioes.asmx/obterInteiroTeorDiscursosPlenario?codSessao={}&numOrador={}&numQuarto={}&numInsercao={}"

for date in daterange:
    sessions_xml = urllib.request.urlopen(sessions_url.format(date[0], date[1]))

    sessions_data = sessions_xml.read()

    sessions_dict = xmltodict.parse(sessions_data)

    sessions = sessions_dict.get('sessoesDiscursos')

    if sessions:
        for session in sessions['sessao']:
            codes = {}
            codes['code'] = session['codigo']
            discourses_list = session['fasesSessao']['faseSessao']['discursos']['discurso']
            discourses_ids = []
            for d in discourses_list:
                discourses = {}
                discourses['speaker_num'] = d['orador']['numero']
                discourses['room_num'] = d['numeroQuarto']
                discourses['insertion_num'] = d['numeroInsercao']
                discourses_ids.append(discourses)
                #write url to file
                urls.append(discourses_url.format(codes['code'], discourses['speaker_num'], discourses['room_num'], discourses['insertion_num']))
            codes['discourses'] = discourses_ids
            sessions_ids.append(codes)

########

discourses_url = "http://www.camara.gov.br/SitCamaraWS/SessoesReunioes.asmx/obterInteiroTeorDiscursosPlenario?codSessao=320.2.54.O&numOrador=2&numQuarto=28&numInsercao=1"

crawled_urls, url_hub = [], urls

async def get_body(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=60) as response:
            return await response.read()

d = []

async def handle_task(task_id, work_queue):
    while not work_queue.empty():
        queue_url = await work_queue.get()
        crawled_urls.append(queue_url)
        body = await get_body(queue_url)
        discourses_dict = xmltodict.parse(body)
        text = striprtf(base64.b64decode(discourses_dict['sessao']['discursoRTFBase64'])).replace('\t', '').replace('\n', '').replace('\x00', '')
        discourses = {'speaker': discourses_dict['sessao']['nome'],
                      'party': discourses_dict['sessao']['partido'],
                      'date': discourses_dict['sessao']['horaInicioDiscurso'],
                      'discourse': text}
        d.append(discourses)
        print(queue_url)

if __name__ == '__main__':
    q = asyncio.Queue()
    [q.put_nowait(url) for url in url_hub]
    loop = asyncio.get_event_loop()
    tasks = [handle_task(task_id, q) for task_id in range(10)]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    with open('data.json', 'w') as outfile:
        json.dump(d, outfile, ensure_ascii=False)
