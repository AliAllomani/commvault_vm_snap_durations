#!/bin/python

"""
Commvault VMware snapshot backups durations
Summarize time taken for software snapshot and disk lease for each VM from vsbkp.log file

Author: https://github.com/AliAllomani
"""

import re
import datetime
import time
import sys,os
import argparse



#vsbkp full Log file name


if len(sys.argv) < 2:
    print("Usage: python "+sys.argv[0]+' vsbkp_path\n\n vsbkp_path\t The path of single vsbkp.log file')
    exit(1)
else:
    log_filename = sys.argv[1]
    if not os.path.isfile(log_filename):
        print('Invalid vsbkp.log file')
        exit(1)


class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

def time_def(t1,t2):
    format = '%m/%d %H:%M:%S'
    startDateTime = datetime.datetime.strptime(t1, format)
    endDateTime = datetime.datetime.strptime(t2, format)
    return (endDateTime - startDateTime).seconds


f = open(log_filename)
vms_snaps = AutoVivification()
vms_lease = AutoVivification()

re1 = '(\d+)[ ]{1,}(\w+)[ ]{1,}(\d{2}\/\d{2} \d{2}:\d{2}:\d{2})[ ]{1}(\d+).+Beginning software snapshot operation for \[(.+)\]\[(.+)\]'
re2 = '(\d+)[ ]{1,}(\w+)[ ]{1,}(\d{2}\/\d{2} \d{2}:\d{2}:\d{2})[ ]{1}(\d+).+Completed software snapshot operation for \[(.+)\]\[(.+)\]'
re3 = '(\d+)[ ]{1,}(\w+)[ ]{1,}(\d{2}\/\d{2} \d{2}:\d{2}:\d{2})[ ]{1}(\d+).+Acquired Disk lease.+\] (.+)\/(.+\.vmdk)'
re4 = '(\d+)[ ]{1,}(\w+)[ ]{1,}(\d{2}\/\d{2} \d{2}:\d{2}:\d{2})[ ]{1}(\d+).+Releasing the Disk lease.+\] (.+)\/(.+\.vmdk)'



for line in f:

    x1 = re.findall(re1,line)
    if(len(x1)):
        key=x1[0][0]+x1[0][1]+x1[0][3]+x1[0][5]
        vms_snaps[key]['process'] = x1[0][0]
        vms_snaps[key]['thread'] = x1[0][1]
        vms_snaps[key]['snap_start'] = x1[0][2]
        vms_snaps[key]['job_id'] = x1[0][3]
        vms_snaps[key]['vm_name'] = x1[0][4]
        vms_snaps[key]['vm_uuid'] = x1[0][5]
        
    else:
        x2 = re.findall(re2,line)
        if(len(x2)):
            key=x2[0][0]+x2[0][1]+x2[0][3]+x2[0][5]
            vms_snaps[key]['process'] = x2[0][0]
            vms_snaps[key]['thread'] = x2[0][1]
            vms_snaps[key]['snap_end'] = x2[0][2]
            vms_snaps[key]['job_id'] = x2[0][3]
            
        else:
            x3 = re.findall(re3,line)
            if(len(x3)):
                key=x3[0][0]+x3[0][1]+x3[0][3]+x3[0][5]
                vms_lease[key]['process'] = x3[0][0]
                vms_lease[key]['thread'] = x3[0][1]
                vms_lease[key]['lease_start'] = x3[0][2]
                vms_lease[key]['job_id'] = x3[0][3]
                vms_lease[key]['vm_name'] = x3[0][4]
                vms_lease[key]['disk'] = x3[0][5]
                
            else:
                x4 = re.findall(re4,line)
                if(len(x4)):
                    key=x4[0][0]+x4[0][1]+x4[0][3]+x4[0][5]
                    vms_lease[key]['process'] = x4[0][0]
                    vms_lease[key]['thread'] = x4[0][1]
                    vms_lease[key]['lease_end'] = x4[0][2]
                    vms_lease[key]['job_id'] = x4[0][3]
                    
f.close()

#Print snaps results to csv file
f_prefix = str(time.time())
output_filename = 'result_'+f_prefix+'_snaps.csv'
f=open(output_filename,'w+')
f.write('Process,Thread,Job ID,VM UUID,VM Name,Snapshot Start,Snapshot End,Snapshot Duration (sec)\n')


for v in vms_snaps:

    f.write(vms_snaps[v]['process']+','+vms_snaps[v]['thread']+','+vms_snaps[v]['job_id']+','
    +vms_snaps[v]['vm_uuid']+','+vms_snaps[v]['vm_name']+','+vms_snaps[v]['snap_start']+','+vms_snaps[v]['snap_end']+','
    +str(time_def(vms_snaps[v]['snap_start'],vms_snaps[v]['snap_end']))+'\n')


f.close()
print('Result file '+output_filename+' created.')

#Print Lease results to csv file
output_filename = 'result_'+f_prefix+'_lease.csv'
f=open(output_filename,'w+')
f.write('Process,Thread,Job ID,VM Name,Disk,Disk Lease Start,Disk Lease End,Disk Lease Duration (sec)\n')
for v in vms_lease:

    f.write(vms_lease[v]['process']+','+vms_lease[v]['thread']+','+vms_lease[v]['job_id']+','
    +vms_lease[v]['vm_name']+','+vms_lease[v]['disk']+','+vms_lease[v]['lease_start']+','+vms_lease[v]['lease_end']+','
    +str(time_def(vms_lease[v]['lease_start'],vms_lease[v]['lease_end']))+'\n')

f.close()
print('Result file '+output_filename+' created.')


#pp = pprint.PrettyPrinter(indent=2)
#pp.pprint(vms)
#print(vms)
    