def load_urls():
    with open('../../../data/urls.txt', 'r') as f:
        urls = f.readlines()

    return urls

def read():
    with open('file.json', 'r') as f:
        for line in f:
            dict_obj = json.loads(line)
