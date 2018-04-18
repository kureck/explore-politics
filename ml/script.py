import pandas as pd
import urllib.request
import xmltodict
import base64
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

daterange = pd.date_range('2014-10-10','2016-01-07' , freq='1M')
daterange = daterange.union([daterange[-1] + 1])
daterange = [d.strftime('%d/%m/%Y') for d in daterange]
daterange = chunks(daterange, 2)

sessions_url = "http://www.camara.gov.br/sitcamaraws/SessoesReunioes.asmx/ListarDiscursosPlenario?dataIni=23/11/2012&dataFim=23/11/2012&codigoSessao=&parteNomeParlamentar=&siglaPartido=&siglaUF="

sessions_xml = urllib.request.urlopen(sessions_url)

sessions_data = sessions_xml.read()

sessions_dict = xmltodict.parse(sessions_data)

sessions = sessions_dict['sessoesDiscursos']['sessao']

sessions_ids = []

for session in sessions:
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
    codes['discourses'] = discourses_ids
sessions_ids.append(codes)

discourses_url = "http://www.camara.gov.br/SitCamaraWS/SessoesReunioes.asmx/obterInteiroTeorDiscursosPlenario?codSessao=320.2.54.O&numOrador=2&numQuarto=28&numInsercao=1"

discourses_xml = urllib.request.urlopen(discourses_url)

discourses_data = discourses_xml.read()

discourses_dict = xmltodict.parse(discourses_data)

text = striprtf(base64.b64decode(discourses_dict['sessao']['discursoRTFBase64'])).replace('\t', '').replace('\n', '').replace('\x00', '')

discourses = {'speaker': discourses_dict['sessao']['nome'],
              'party': discourses_dict['sessao']['partido'],
              'date': discourses_dict['sessao']['horaInicioDiscurso'],
              'discourse': text}

if __name__ == '__main__':
    print(sessions_ids)
    print()
    print(discourses)
