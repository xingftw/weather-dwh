import csv, sys, re

datasources = ['aberporthdata',
               'armaghdata',
               'ballypatrickdata',
               'bradforddata',
               'cambridgedata',
               'heathrowdata',
               'oxforddata','southamptondata']

processed_list = []
for file in datasources:
   z = open(file+".txt", "r").read().splitlines()


   for line in z[7:]:
       list = line.split()  ##turns into list at split at colon
       new_list = [re.sub(r"---|\*", "", x) for x in list[:7]]
       new_list.append(file.replace('data',''))
       processed_list.append(new_list)

with open("historical_weather_stats.csv", 'w', newline='\n') as myfile:
  wr = csv.writer(myfile)
  wr.writerows(processed_list)
