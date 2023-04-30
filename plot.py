# list files in a directory
import os
import matplotlib.pyplot as plt
import pandas as pd

datanames = [
    {
        'name': 'local1.txt',
        "ref": "source=18, count=12, replicas=1, localkv, junk=5",
        'colName': 'localKV1',
    },
    {
        'name': 'local2.txt',
        "ref": "source=9, count=12, replicas=1, localkv, junk=5",
        'colName': 'localKV2',
    },
    {
        'name': 'local3.txt',
        "ref":"source=9, count=12, replicas=2, localkv, junk=5, migrate=1682491160567",
        "migrate": "1682491160567",
        'colName': 'localKV3',
    },
    {
        'name': 'hybrid1.txt',
        "ref": "source=18, count=12, replicas=1, hybridkv, junk=5",
        "colName": "hybridKV1",
    },
    {
        "name": "hybrid2.txt",
        "ref": "source=9, count=12, replicas=2, hybridkv, junk=5, cache=no",
        "colName": "hybridKV2",
    },
    {
        "name": "hybrid3.txt",
        "ref": "source=9, count=12, replicas=2, hybridkv, junk=5, cache=yes",
        "colName": "hybridKV3",
    },
    {
        "name": "hybrid4.txt",
        "ref": "source=9, count=12, replicas=2, hybridkv, junk=5, cache=yes, migrate=1682536203097",
        "migrate": "1682536203097",
        "colName": "hybridKV4",
    },
    {
        "name": "hybrid5.txt",
        "ref": "source=9, count=12, replicas=2, hybridkv, junk=5, cache=yes, migrate=yes, migrate=1682546002468",
        "migrate": "1682546002468",
        "colName": "hybridKV5",
    }
]

columns = list(map(lambda x: x['colName']+":" + x['ref'] , datanames))

# columns.insert(0, 'timestamp')

# draw_df = pd.DataFrame(columns=columns)
for i,dataname in enumerate(datanames):
    df = pd.read_csv('zout/'+dataname['name'], names=['timestamp', 'time_spent'])
    # get the timestamp of the first line
    first_timestamp = df['timestamp'][0]
    # for each row, timestamp = timestamp - first_timestamp
    df['timestamp'] = df['timestamp'].apply(lambda x: x - first_timestamp)
    if dataname['name'] in ['hybrid3.txt','hybrid2.txt']:
        # remove all rows where the first column is greater than 2000 and the second column is greater than 500
        df = df[(df['timestamp'] < 2000) & (df['time_spent'] < 800)]
    if 'migrate' in dataname:
        # add a dot closest to where the migration happens
        happen = int(dataname['migrate']) - first_timestamp
        print(dataname['name'], happen)

    df.set_index('timestamp', inplace=True)
    plt.plot(df.index, df['time_spent'])

ax = plt.gca()
ax.set_ylim([0,500])
# add legend
plt.legend(columns)


plt.xlabel('Timestamp (ms)')
plt.ylabel('End to End Latency (ms)')
plt.title('Change of Time Spent over Real-World Time')
plt.grid(True)
plt.show()
