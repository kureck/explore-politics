import urllib.request
import xml.etree.ElementTree as ET
import base64

# https://github.com/pmarkun/discursos-genero
# http://www.camara.gov.br/sitcamaraws/SessoesReunioes.asmx/ListarDiscursosPlenario?dataIni=23/11/2012&dataFim=23/11/2012&codigoSessao=&parteNomeParlamentar=&siglaPartido=&siglaUF=
# tem que pegar o código aqui pra poder criar a url abaixo

xml = urllib.request.urlopen("http://www.camara.gov.br/SitCamaraWS/SessoesReunioes.asmx/obterInteiroTeorDiscursosPlenario?codSessao=320.2.54.O&numOrador=2&numQuarto=28&numInsercao=1")

data = xml.read()

tree = ET.fromstring(data)

discourse = tree.find('discursoRTFBase64')

discourse_text = base64.b64decode(discourse.text)
