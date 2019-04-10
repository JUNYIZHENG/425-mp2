import pandas as pd
import glob
import matplotlib.pyplot as plt

pd.set_option('display.float_format', lambda x: '%.3f' % x)


def read_to_df(folder):
    file_name = glob.glob('{folder}/*.txt'.format(folder=folder), recursive=False)
    # print(file_name)
    file_list = []
    for name in file_name:
        file = pd.read_csv(name, sep=',', header=None)
        file_list.append(file)
    file_concat = pd.concat(file_list)
    # print(file_concat)
    return file_concat


trans = read_to_df('trans')

trans.columns=['transaction','timestamp','delay']
# print(trans)
# number of nodes
# uni_trans = pd.unique(trans['transaction'])
# print(uni_trans)
def node_num():
# print(trans['transaction'])
    trans_count = pd.DataFrame(trans[['timestamp','delay']].groupby('timestamp').count())
#     trans_count = pd.DataFrame(trans.count())
#     trans_count = trans_count.reset_index()
    trans_count.columns = ['node_num']
    # print(trans_count)
    fig = plt.figure(0)
    plt.plot(trans_count,'b.',markersize=3)
    plt.title('Reachability of transactions')
    plt.xlabel('Timestamp')
    plt.ylabel('Number of nodes')
    plt.savefig('fig_trans_nodenum.png')
    plt.show()
    plt. close(0)
node_num()


# delay(min max median)
def delay():
    # trans_delay = pd.DataFrame(trans[['timestamp','delay']])
    # trans['timestamp'] = str(trans['timestamp'])
    # print(trans)
    trans_min = pd.DataFrame(trans[['timestamp','delay']].groupby('timestamp').min())
    trans_min.columns = ['min_delay']
    trans_max = pd.DataFrame(trans[['timestamp','delay']].groupby('timestamp').max())
    trans_max.columns = ['max_delay']
    trans_median = pd.DataFrame(trans[['timestamp','delay']].groupby('timestamp').median())
    trans_median.columns = ['median_delay']
    # print(trans_min,trans_max,trans_median)
    # trans_delay = pd.merge(trans_min, trans_max, on='timestamp')
    # trans_delay = pd.merge(trans_delay, trans_median, on='timestamp')
    # print(trans_delay)
    # trans_delay = trans_delay.T
    # print(trans_delay)
    fig = plt.figure()
    # plt.plot(trans_delay,'.')
    plt.plot(trans_min,'b.',label='Min',markersize=3)
    plt.plot(trans_max,'c.',label='Max',markersize=3)
    plt.plot(trans_median,'r.',label='Median',markersize=3)
    plt.legend()
    # trans_delay.boxplot(medianprops={'marker':'D','markerfacecolor':'red'})
    # plt.boxplot(trans_delay,whis=100)  # force the whiskers to the min and max of the data
    plt.title('propagation delay of transactions')
    plt.xlabel('Timestamp')
    plt.ylabel('propagation delay(s)')
    plt.savefig('fig_trans_delay.png')
    plt.close()
delay()

pd.set_option('display.float_format', lambda x: '%.25f' % x)
def read_to_df(folder):
    file_name = glob.glob('{folder}/*.txt'.format(folder=folder), recursive=False)
    file_list = []
    for name in file_name:
        file = pd.read_csv(name, sep=',', header=None)
        file_list.append(file)
    file_concat = pd.concat(file_list)
    file_uniq = file_concat[0].unique()
    file_concat = file_concat[file_concat[0] == file_uniq]
    return file_concat


df = read_to_df('bandwidth')
df = pd.DataFrame(df)
df.columns = ['time','bandwidth']
ori_bw = min(df['bandwidth'])
df['bandwidth'] -= ori_bw
df.sort_values("time", inplace=True)
# print(df)
a = df['time']
b = df['bandwidth']
fig = plt.figure()
plt.plot(a, b)
plt.title('Bandwidth by time')
plt.xlabel('time(s)')
plt.ylabel('bandwidth used(M)')
plt.savefig('fig_bandwidth.png')
