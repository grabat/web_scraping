import lxml.html
data = open('result.html', 'r').read()
doc = lxml.html.fromstring(data)
print(doc)
