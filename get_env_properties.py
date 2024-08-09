import os

os.environ['enrv'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
enrv = os.environ['enrv']

appName = 'Youtube project'

current = os.getcwd()

src_olap = current + '\Source\olap'
src_oltp = current + '\Source\oltp'

city_path = 'Output\cities'
prescriber = 'Output\prescribers'
