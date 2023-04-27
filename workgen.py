import random, json, string
def make_json_line():
    return json.dumps({
        "id": random.randint(1, 100),
        "server_name": "server-" + str(random.randint(1, 100000)),
    })

def gen_random_str(len:int=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=len))

with open("datarandom10000.txt",'w') as f:
    for i in range(10000):
        f.write(make_json_line()+"\n")

