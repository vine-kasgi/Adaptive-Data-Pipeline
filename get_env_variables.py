import os

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
envn = os.environ['envn']

appName = 'My First Pipeline'

current = os.getcwd()

src_olap = current + '\Source\OLAP'

src_oltp = current + '\Source\OLTP'

# print(src_olap)