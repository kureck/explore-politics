from pandas import date_range
import urllib.request
import xmltodict
import base64
import aiohttp
import asyncio
import json

def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

def write(url):
    with open('../../data/urls.txt', 'a') as f:
        f.write('{}\n'.format(url))

daterange = date_range('2000-01-01','2018-04-15' , freq='1M')
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
                try:
                    discourses = {}
                    discourses['speaker_num'] = d['orador']['numero']
                    discourses['room_num'] = d['numeroQuarto']
                    discourses['insertion_num'] = d['numeroInsercao']
                    discourses_ids.append(discourses)
                    #write url to file
                    url = discourses_url.format(codes['code'], discourses['speaker_num'], discourses['room_num'], discourses['insertion_num'])
                    write(url)
                    print("URL: {} written.".format(url))
                    urls.append(url)
                except TypeError:
                    print(d)
                    break
            codes['discourses'] = discourses_ids
            sessions_ids.append(codes)

if __name__ == '__main__':
    pass
