import pandas as pand

# load packages information from .csv file

file = pand.read_csv('in.csv',sep=',')

print('Lets get it )')

print()

# select rows, which contains only TCP protocol data

filedf = pand.DataFrame(file)

# group by the protocol

tcpgroup = filedf.groupby('Protocol')

tcp = tcpgroup.get_group('TCP')

tcpupdate = pand.concat([tcp['Source'],tcp['Destination'],tcp['Source_port'],tcp['Dest_port']],axis=1)

print('TCP exchange stats')

print(tcpupdate)

# print the TCP port stats

tcpupdate = pand.DataFrame(tcpupdate.groupby(['Source','Destination','Source_port','Dest_port'])['Source'].count())

tcpupdate = tcpupdate.rename(columns = { 'Source' : 'Requests' })

# output the portstats

print('Port stats:')

print(tcpupdate)

tcpupdate.to_csv('Output.csv')

# select most recent pair between them

print('The most active pair was:')

stats = pand.concat([tcp['Source'],tcp['Destination']],axis=1)

stats['uniq'] = stats['Source'] + ' ---> ' + stats['Destination']

count = stats['uniq'].value_counts() [:1]

print(count)


# created by Sergey Besedin