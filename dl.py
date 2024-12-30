#!/data/sharedata/fnf/Python-3.12.4/installed/bin/python3
# # -*- coding: utf-8 -*-
#Author         :Mapoet.Niphy
#Date & Time     :2024-7-22 14:27:50
#Loc            :TJYY

from datetime import  datetime, timedelta
import json,sys,os,time,threading,math,shutil,requests,requests_ftp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import http.client,copy
username_whu='anonymous'
password_whu='xxxx'
iprocs=2
exitFlag=0
tasks=[]
lock = threading.Lock()
stations=["AH223","AL945","AN438","AS00Q","AT138","AU930","BP440","BLJ03","BJJ32","BVJ03",
        "BC840","BR52P","BV53Q","CXM9B","CAJ2M","CN53L","CGK21","CB53N","CH833","RL052",
        "CS31K","CO764","CS839","DW41K","DB049","DS932","EA653","EG931","EI764","EA036",
        "FF051","FZA0M","GA313","GA762","GM037","GSJ53","GR13L","GU513","HA419","HAJ43",
        "HE13N","HO54K","IC437","AC843","IF843","IL008","IR352","JB57N","JJ433","JI91J",
        "JR055","KL154","KI939","KB547","KS759","KR835","KI167","TO535","KJ609","LA42Q",
        "LM42B","LV12P","LL721","MU12K","MA560","ME929","MHJ45","MH453","MA155","MO155",
        "MO155","MU230","MU834","NQJ61","NI135","ND61R","NDA81","NI63_","NO369","NV355",
        "OK426","OL246","SN437","PE43K","PK553","PF765","PSJ5J","PQ052","PA836","THJ77",
        "PRJ18","RM041","RO041","EB040","RV149","SH266","VT139","SMK29","SA418","SAA0K",
        "SMJ67","SO148","SH42_","SQ832","THJ76","TV51R","TM308","TR169","TR170","TNJ2O",
        "TUJ2O","TZ362","WA619","WP937","WI937","MZ152","WU430","XI434","YA462","ZH466",
        "ZS36R"]
def session_download(obsfile:str,outfile:str,input:dict={},username:str='anonymous', password:str='',max_attempts:int=5):
    with requests.Session() as session:
        retry = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.mount('ftp://', adapter)
        session.mount('ftps://', adapter)
        if os.path.exists(outfile):os.remove(outfile)
        with open(outfile,'w') as f:
            header=0
            for ista in range(0,len(stations)):
                attempt = 0
                while attempt < max_attempts:
                    try:
                        inputs=copy.deepcopy(input)
                        inputs['ursiCode']=stations[ista]
                        r=session.request('get', url=obsfile,params=inputs,timeout=15)
                        r.raise_for_status()
                        if r.ok:
                                lines=r.content.decode().split('\n')
                                if len(lines)<58:
                                    print('err(%s,%s,%s):%s'%(stations[ista],inputs['fromDate'],inputs['toDate'],
                                                              'ERROR: No data found for requested period'))
                                    break
                                its=lines[4].split(',')[0].split()
                                lat=its[3];lon=its[4]
                                if header==0:
                                    f.write('%10s %8s %8s %s\n'%('stations','lat','lon',lines[57][1:]))
                                    header=1
                                for line in lines[58:]:
                                    if len(line)>50:
                                        f.write('%10s %8s %8s %s\n'%(stations[ista],lat,lon,line))
                                break
                        else:
                            print('err:%s'%str(r.status_code))
                    except Exception as e:
                        print('session_download(%s):[%d]%s'%(outfile,attempt,str(e)))
                        attempt=attempt+1
            return True
        return False

class taskThread (threading.Thread):
    def __init__(self,obsfile:str,username:str='anonymous', password:str='',max_attempts:int=5):
        threading.Thread.__init__(self)
        self.obsfile=obsfile
        self.username=username
        self.password=password
        self.max_attempts=max_attempts
    def run(self):
        global tasks
        while not exitFlag:
            if not len(tasks)==0:
                lock.acquire()
                task=tasks.pop()
                lock.release()
                rt=session_download(obsfile=self.obsfile,outfile=task['of'],input=task['input'],
                    username=self.username,password=self.password,max_attempts=self.max_attempts)
                if rt:
                    print('%s S'%task['of'])
                else:
                    print('%s F'%task['of'])
            else:
                break
            time.sleep(0.2)
                    
if __name__ == "__main__":
    datadir='./ionosonde'
    url='https://lgdc.uml.edu/common/DIDBGetValues'
    if len(sys.argv)<3:
        print('need st and et')
        exit(-1)
        st=datetime(year=2012,month=7,day=2)
        et=st+timedelta(days=1)
    else:
        #format = "%Y-%m-%d %H:%M:%S"
        st=datetime.strptime(sys.argv[1],'%Y-%m-%d')
        et=datetime.strptime(sys.argv[2],'%Y-%m-%d')
    print('get ionosonde data from %s to %s'%(st,et))
    """
    view-source:https://lgdc.uml.edu/common/DIDBGetValues?ursiCode=AH223&charName=foF2,foF1,foE,foEs,fbEs,foEa,foP,fxI,MUFD,MD,hF2,hF,hE,hEs,hEa,hP,TypeEs,hmF2,hmF1,hmE,zhalfNm,yF2,yF1,yE,scaleF2,B0,B1,D1,TEC,FF,FE,QF,QE,fmin,fminF,fminE,fminEs,foF2p&DMUF=3000&fromDate=2012%2F07%2F02+21%3A00%3A00&toDate=2012%2F07%2F04+03%3A00%3A00
    """
    for iday in range(0,(et-st).days+1):
        tst = st + timedelta(days=iday)
        tet = tst + timedelta(days=1)
        tsts='%4.4d/%2.2d/%2.2d %2.2d:00'%(tst.year,tst.month,tst.day,tst.hour)
        tets='%4.4d/%2.2d/%2.2d %2.2d:00'%(tet.year,tet.month,tet.day,tet.hour)
        input={
            'charName':"foF2,foF1,foE,foEs,fbEs,foEa,foP,fxI,MUFD,MD,hF2,hF,hE,hEs,hEa,hP,TypeEs,hmF2,hmF1,hmE,zhalfNm,yF2,yF1,yE,scaleF2,B0,B1,D1,TEC,FF,FE,QF,QE,fmin,fminF,fminE,fminEs,foF2p",
            'DMUF':"3000",
            'fromDate':tsts,
            'toDate':tets
        }
        tsts='%4.4d%2.2d%2.2d%2.2d'%(tst.year,tst.month,tst.day,tst.hour)
        tets='%4.4d%2.2d%2.2d%2.2d'%(tet.year,tet.month,tet.day,tet.hour)
        tasks.append({
            'of':'%s/is%s_%s.txt'%(datadir,tsts,tets),
            'input':input
        })
    procs=[]
    if len(tasks)<iprocs:
        iprocs=len(tasks)
    for m in range(0,iprocs):
        procs.append(taskThread(obsfile=url,username=username_whu,password=password_whu,max_attempts=5))
        procs[m].start()
    while not len(tasks)==0:
         time.sleep(1)
    exitFlag = 1
    for n in range(0,len(procs)):
         procs[n].join()