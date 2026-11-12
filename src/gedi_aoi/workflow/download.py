"""Resumable full GEDI V3 selected-attribute download with live local progress.
Authoritative manifest: metadata/full_download/manifest.sqlite.
Run --self-test before launch; --run starts/resumes; --progress opens monitor.
"""
import sys,pathlib,json,time,datetime,threading,sqlite3,concurrent.futures,urllib.request,urllib.parse,urllib.error,uuid,io,importlib.util,csv,shutil,os,argparse
ROOT=pathlib.Path(os.environ['GEDI_PROJECT_ROOT']).resolve()
import numpy as np,h5py
META=ROOT/'metadata/full_download';OUT=ROOT/'data/full_download';STAGE=ROOT/'data/full_download_staging'
for p in [META,OUT,STAGE]:p.mkdir(exist_ok=True)
DB=META/'manifest.sqlite';PROGRESS=META/'progress.json';CONTROL=META/'control.json'
CONFIG=json.loads((ROOT/'project.json').read_text());START=CONFIG['start'];END=CONFIG['end']
spec=importlib.util.spec_from_file_location('normal',pathlib.Path(__file__).with_name('shared.py'));normal=importlib.util.module_from_spec(spec);spec.loader.exec_module(normal)
TOKEN=None;COLS=normal.COLS
BEAMS=normal.BEAMS;LOCK=threading.RLock();SESSION_BYTES=0;STOP=threading.Event();PHASE='Starting';ACTIVE={};TREE=None

def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def connect():
    db=sqlite3.connect(DB,timeout=60);db.row_factory=sqlite3.Row;return db
def init():
    with connect() as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY,product TEXT,granule_id TEXT,source_name TEXT,start_time TEXT,metadata TEXT,status TEXT DEFAULT "pending",attempts INTEGER DEFAULT 0,harmony_id TEXT,output TEXT,shots INTEGER DEFAULT 0,bytes INTEGER DEFAULT 0,error TEXT,updated TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY,value TEXT)')
        db.execute('INSERT OR IGNORE INTO settings VALUES (?,?)',('started_utc',now()))
        db.execute('INSERT OR IGNORE INTO settings VALUES (?,?)',('temporal',START+','+END))
def setting(key,default=None):
    with connect() as db:r=db.execute('SELECT value FROM settings WHERE key=?',(key,)).fetchone()
    return r[0] if r else default
def setsetting(key,val):
    with connect() as db:db.execute('INSERT OR REPLACE INTO settings VALUES (?,?)',(key,str(val)))
def update_job(jid,**values):
    values['updated']=now()
    with connect() as db:db.execute('UPDATE jobs SET '+','.join(k+'=?' for k in values)+' WHERE id=?',(*values.values(),jid))
def pause_check():
    while True:
        try:paused=json.loads(CONTROL.read_text()).get('paused',False)
        except (OSError,ValueError):paused=False
        if not paused:return
        time.sleep(1)
def fetch(url,auth=False,data=None,content_type=None,limit=128*1024*1024):
    global SESSION_BYTES
    for attempt in range(4):
        pause_check()
        if shutil.disk_usage(ROOT).free<CONFIG.get('min_free_gib',3)*1024**3:raise RuntimeError('Less than 3 GiB free; download stopped for disk space')
        headers={'Accept':'*/*'}
        if auth:headers['Authorization']='Bearer '+TOKEN
        if content_type:headers['Content-Type']=content_type
        req=urllib.request.Request(url,data=data,headers=headers)
        try:
            with urllib.request.build_opener(normal.Redirect()).open(req,timeout=120) as r:result=r.read(limit+1)
            if len(result)>limit:raise RuntimeError('Response exceeded size safeguard; full-granule fallback is disabled')
            with LOCK:SESSION_BYTES+=len(result)
            return result
        except urllib.error.HTTPError as exc:
            if exc.code not in [429,500,502,503,504] or attempt==3:raise RuntimeError('NASA HTTP '+str(exc.code)) from None
        except (TimeoutError,urllib.error.URLError,ConnectionError):
            if attempt==3:raise RuntimeError('Network request failed after four attempts') from None
        time.sleep(min(30,2**(attempt+1)))

def load_cells():
    global TREE,GEOMS,AOI_IDS,DOMAIN_IDS,TRANSFORM,WGS_BOUNDS
    import pyogrio.raw,shapely,pyproj
    meta,_,wkb,fields=pyogrio.raw.read(ROOT/'data/study_area/aoi_units.gpkg',columns=['aoi_id','domain_id'])
    data=dict(zip(meta['fields'],fields));AOI_IDS=data['aoi_id'];DOMAIN_IDS=data['domain_id']
    GEOMS=shapely.from_wkb(wkb);TREE=shapely.STRtree(GEOMS);TRANSFORM=pyproj.Transformer.from_crs(4326,meta['crs'],always_xy=True)
    domains=json.loads((ROOT/'data/study_area/aoi_domains.geojson').read_text());WGS_BOUNDS=shapely.total_bounds([shapely.geometry.shape(f['geometry']) for f in domains['features']])
    if len(set(AOI_IDS))!=len(AOI_IDS):raise RuntimeError('Duplicate AOI IDs')

def assign(lon,lat):
    import shapely
    candidate=np.flatnonzero(np.isfinite(lon)&np.isfinite(lat)&(lon>=WGS_BOUNDS[0])&(lon<=WGS_BOUNDS[2])&(lat>=WGS_BOUNDS[1])&(lat<=WGS_BOUNDS[3]))
    if not len(candidate):return np.array([],dtype=int),np.array([],dtype=int),0
    x,y=TRANSFORM.transform(lon[candidate],lat[candidate]);pairs=TREE.query(shapely.points(x,y),predicate='intersects')
    if not pairs.size:return np.array([],dtype=int),np.array([],dtype=int),0
    order=np.lexsort((pairs[1],pairs[0]));a=pairs[0,order];b=pairs[1,order];keep=np.r_[True,a[1:]!=a[:-1]]
    ambiguous=int(len(a)-keep.sum())
    return candidate[a[keep]],b[keep],ambiguous

def selected(product,beam=None):
    from gedi_aoi.selection import select
    cap=json.loads((ROOT/'metadata'/(product+'_harmony_capabilities.json')).read_text())
    return [v for v in select(CONFIG,product,cap) if beam is None or v.strip('/').split('/')[0]==beam]

def attrs(src,dst):
    for k,v in src.attrs.items():
        if k not in ['DIMENSION_LIST','REFERENCE_LIST','CLASS','NAME','_Netcdf4Coordinates','_Netcdf4Dimid']:dst.attrs[k]=v

def map_fields(group,ci):
    group.create_dataset('aoi_id',data=np.array(AOI_IDS[ci],dtype=h5py.string_dtype()),compression='gzip')
    group.create_dataset('domain_id',data=np.array(DOMAIN_IDS[ci],dtype=h5py.string_dtype()),compression='gzip')
def validate(path):
    count=0;seen=set()
    with h5py.File(path,'r') as h:
        if not h.attrs.get('complete',False):raise RuntimeError('Incomplete output')
        for beam in BEAMS:
            if beam not in h:continue
            g=h[beam];ids=g['shot_number'][...].reshape(-1)
            if len(ids)!=len(g['aoi_id']):raise RuntimeError('Cell-assignment length mismatch')
            if len(set(map(int,ids)))!=len(ids) or seen.intersection(map(int,ids)):raise RuntimeError('Duplicate shot identifiers')
            seen.update(map(int,ids));count+=len(ids)
            for field in ['rh','rch']:
                if field in g and g[field].shape!=(len(ids),101):raise RuntimeError('Unexpected height profile dimensions')
            for field in ['cover_z','pai_z','pavd_z']:
                if field in g and (g[field].ndim!=2 or g[field].shape[0]!=len(ids)):raise RuntimeError('Unexpected foliage profile dimensions')
    return count

def clip_source(source,dest,product,source_name):
    total=0;ambiguous=0
    with h5py.File(source,'r') as src,h5py.File(dest,'w') as dst:
        dst.attrs['product']=product;dst.attrs['source_granule']=source_name;dst.attrs['quality_filtered']=False
        for beam in BEAMS:
            if beam not in src or 'shot_number' not in src[beam]:continue
            g=src[beam];ids=g['shot_number'][...].reshape(-1)
            ix,ci,multi=assign(g['lon_lowestmode'][...].reshape(-1),g['lat_lowestmode'][...].reshape(-1));ambiguous+=multi
            if not len(ix):continue
            target=dst.create_group(beam);total+=len(ix)
            def copy(name,obj):
                if not isinstance(obj,h5py.Dataset):return
                if obj.ndim and obj.shape[0]==len(ids):data=obj[...][ix]
                elif obj.ndim==0 or obj.size<=101:data=obj[...]
                else:raise RuntimeError('Unexpected ancillary dimension: '+name)
                d=target.create_dataset(name,data=data,compression='gzip' if np.ndim(data) else None,shuffle=bool(np.ndim(data)));attrs(obj,d)
            g.visititems(copy);map_fields(target,ci)
        dst.attrs['boundary_tie_count']=ambiguous;dst.attrs['complete']=True
    return total

def inventory():
    global PHASE
    PHASE='Discovering available granules'
    domains=list(csv.DictReader((ROOT/'metadata/aoi_summary.csv').open()))
    for product,cid in COLS.items():
        for domain in domains:
            marker='inventory_'+product+'_'+domain['domain_id']
            if setting(marker)=='complete':continue
            box=','.join(domain[k] for k in ['west','south','east','north'])
            page=1
            while True:
                PHASE=f'Discovering {product} / {domain["domain_id"]} / page {page}'
                params={'collection_concept_id':cid,'bounding_box':box,'temporal':START+','+END,'page_size':200,'page_num':page,'sort_key':'start_date'}
                result=json.loads(fetch('https://cmr.earthdata.nasa.gov/search/granules.umm_json?'+urllib.parse.urlencode(params)))
                items=result.get('items',[])
                with connect() as db:
                    for g in items:
                        name=g['umm']['GranuleUR'];gid=g['meta']['concept-id'];dt=g['umm'].get('TemporalExtent',{}).get('RangeDateTime',{}).get('BeginningDateTime','')
                        db.execute('INSERT OR IGNORE INTO jobs(id,product,granule_id,source_name,start_time,metadata,updated) VALUES (?,?,?,?,?,?,?)',(product+'_'+gid,product,gid,name,dt,json.dumps(g),now()))
                if len(items)<200:break
                page+=1
            setsetting(marker,'complete')
    for p in COLS:setsetting('selected_variables_'+p,json.dumps(selected(p)))
    setsetting('inventory_complete','true');PHASE='Downloading selected attributes'

def multipart(fields,shape=None):
    boundary='gedi'+uuid.uuid4().hex;body=bytearray()
    for k,v in fields:body.extend(('--'+boundary+'\r\nContent-Disposition: form-data; name="'+k+'"\r\n\r\n'+v+'\r\n').encode())
    if shape:
        body.extend(('--'+boundary+'\r\nContent-Disposition: form-data; name="shape"; filename="study_boxes.geojson"\r\nContent-Type: application/geo+json\r\n\r\n').encode());body.extend(shape);body.extend(b'\r\n')
    body.extend(('--'+boundary+'--\r\n').encode());return bytes(body),'multipart/form-data; boundary='+boundary

def harmony(job,part):
    jid=job['harmony_id']
    if not jid:
        polygons=[]
        for d in csv.DictReader((ROOT/'metadata/aoi_summary.csv').open()):
            w,s,e,n=[float(d[k]) for k in ['west','south','east','north']];polygons.append([[[w,s],[e,s],[e,n],[w,n],[w,s]]])
        shape=json.dumps({'type':'FeatureCollection','features':[{'type':'Feature','properties':{},'geometry':{'type':'MultiPolygon','coordinates':polygons}}]}).encode()
        fields=[('variable',','.join(selected(job['product']))),('granuleId',job['granule_id']),('forceAsync','true'),('skipPreview','true'),('maxResults','1'),('ignoreErrors','false')]
        # Use the tested bbox service. Restrict to domain_code domains intersecting the granule footprint.
        import shapely
        geometry=json.loads(job['metadata'])['umm'].get('SpatialExtent',{}).get('HorizontalSpatialDomain',{}).get('Geometry',{})
        footprints=[]
        for polygon in geometry.get('GPolygons',[]):
            coords=[(p['Longitude'],p['Latitude']) for p in polygon['Boundary']['Points']]
            footprints.append(shapely.make_valid(shapely.Polygon(coords)))
        boxes=[]
        for d in csv.DictReader((ROOT/'metadata/aoi_summary.csv').open()):
            box=[float(d[k]) for k in ['west','south','east','north']]
            if not footprints or any(shapely.intersects(p,shapely.box(*box)) for p in footprints):boxes.append(box)
        if not boxes:boxes=[[float(d[k]) for k in ['west','south','east','north']] for d in csv.DictReader((ROOT/'metadata/aoi_summary.csv').open())]
        west=min(b[0] for b in boxes);south=min(b[1] for b in boxes);east=max(b[2] for b in boxes);north=max(b[3] for b in boxes)
        fields.extend([('subset',f'lon({west}:{east})'),('subset',f'lat({south}:{north})')])
        body,ctype=multipart(fields)
        url='https://harmony.earthdata.nasa.gov/'+COLS[job['product']]+'/ogc-api-coverages/1.0.0/collections/parameter_vars/coverage/rangeset'
        response=json.loads(fetch(url,True,body,ctype));jid=response['jobID'];update_job(job['id'],harmony_id=jid)
    began=time.monotonic()
    while True:
        pause_check();state=json.loads(fetch('https://harmony.earthdata.nasa.gov/jobs/'+jid,True))
        with LOCK:ACTIVE[job['id']]=job['product']+' NASA '+str(state.get('status'))+' '+str(state.get('progress',0))+'%'
        if state.get('status')=='successful':break
        if state.get('status') in ['failed','canceled']:
            update_job(job['id'],harmony_id=None)
            raise RuntimeError('Harmony '+str(state.get('status'))+': '+str(state.get('message',''))[:400])
        if time.monotonic()-began>1800:raise RuntimeError('Harmony job still pending after 30 minutes; retained for resume')
        time.sleep(5)
    links=[x for x in state.get('links',[]) if x.get('rel')=='data' and urllib.parse.urlparse(x['href']).path.lower().endswith(('.h5','.hdf5','.nc','.nc4'))]
    if not links:
        explicit_empty=state.get('numInputGranules')==1 and any('No data found to process' in w.get('message','') for w in state.get('warnings',[]))
        if explicit_empty:
            with h5py.File(part,'w') as h:
                h.attrs['product']=job['product'];h.attrs['source_granule']=job['source_name'];h.attrs['complete']=True;h.attrs['quality_filtered']=False
                h.attrs['empty_evidence']='NASA successful spatial subset with explicit No data found to process warning'
                h.attrs['harmony_job_id']=jid
            return 0
        raise RuntimeError('Successful Harmony job returned no HDF5 data link; manual review required')
    if len(links)!=1:raise RuntimeError('Unexpected multiple output links for one granule')
    data=fetch(links[0]['href'],True)
    if data[:8]!=b'\x89HDF\r\n\x1a\n':raise RuntimeError('Staged download is not HDF5')
    temp=STAGE/(job['id']+'.h5');temp.write_bytes(data)
    count=clip_source(temp,part,job['product'],job['source_name'])
    # Only this validated, known temporary file is removed.
    if temp.parent.resolve()!=STAGE.resolve():raise RuntimeError('Unexpected temporary path')
    temp.unlink();return count

def opendap(job,part):
    g=json.loads(job['metadata']);base=next((u['URL'] for u in g['umm']['RelatedUrls'] if u.get('Subtype')=='OPENDAP DATA'),None)
    if not base:raise RuntimeError('No published OPeNDAP endpoint')
    def dap(ce):
        data=fetch(base+'.dap.nc4?'+urllib.parse.urlencode({'dap4.ce':ce}),True)
        if data[:8]!=b'\x89HDF\r\n\x1a\n':raise RuntimeError('OPeNDAP response is not NetCDF4')
        return h5py.File(io.BytesIO(data),'r')
    # Some source granules contain fewer than eight beams. Query their actual schema.
    import xml.etree.ElementTree as ET
    dmr=fetch(base+'.dmr.xml',True)
    tree=ET.fromstring(dmr)
    available=[g.attrib['name'] for g in tree if g.tag.rsplit('}',1)[-1]=='Group' and g.attrib.get('name') in BEAMS]
    if not available:raise RuntimeError('Source metadata contains no GEDI beam groups')
    with h5py.File(part,'a') as dst:
        dst.attrs['product']=job['product'];dst.attrs['source_granule']=job['source_name'];dst.attrs['quality_filtered']=False
        dst.attrs['source_available_beams']=json.dumps(available)
        dst.attrs['source_absent_beams']=json.dumps([b for b in BEAMS if b not in available])
        for beam in available:
            if beam in dst and dst[beam].attrs.get('complete',False):continue
            with LOCK:ACTIVE[job['id']]=job['product']+' '+beam+' coordinates / metrics'
            with dap('/'+beam+'/lat_lowestmode;/'+beam+'/lon_lowestmode') as coords:
                lon=coords[beam]['lon_lowestmode'][...].reshape(-1);lat=coords[beam]['lat_lowestmode'][...].reshape(-1)
            ix,ci,multi=assign(lon,lat)
            if not len(ix):continue
            fields=[p[len(beam)+2:] for p in selected(job['product'],beam)]
            # Nearby runs are coalesced to reduce requests; extra rows are removed immediately.
            runs=np.split(ix,np.flatnonzero(np.diff(ix)>64)+1);values={v:[] for v in fields};attributes={}
            for run in runs:
                a,b=int(run[0]),int(run[-1]);ce=';'.join('/'+beam+'/'+v+'['+str(a)+':'+str(b)+']' for v in fields)
                with dap(ce) as src:
                    for v in fields:
                        ds=src[beam][v];data=ds[...]
                        if data.shape[0]!=b-a+1:raise RuntimeError('Unexpected OPeNDAP shot dimension')
                        values[v].append(data[run-a]);attributes[v]={k:val for k,val in ds.attrs.items() if k not in ['DIMENSION_LIST','REFERENCE_LIST','CLASS','NAME','_Netcdf4Coordinates','_Netcdf4Dimid']}
            if beam in dst:del dst[beam] # Replace an incomplete group only inside this resumable .part file.
            target=dst.create_group(beam)
            for v,arrays in values.items():
                ds=target.create_dataset(v,data=np.concatenate(arrays,axis=0),compression='gzip',shuffle=True)
                for k,val in attributes[v].items():ds.attrs[k]=val
            map_fields(target,ci);target.attrs['boundary_tie_count']=multi;target.attrs['complete']=True;dst.flush()
        dst.attrs['complete']=True

def process(job):
    with LOCK:ACTIVE[job['id']]=job['product']+' starting'
    update_job(job['id'],status='running',attempts=job['attempts']+1,error=None)
    product=job['product'];folder=OUT/product;folder.mkdir(exist_ok=True)
    dest=folder/(job['source_name']+'.h5');part=dest.with_suffix('.h5.part')
    try:
        pause_check()
        if dest.exists():count=validate(dest)
        else:
            if product.startswith('L2'):harmony(job,part)
            else:opendap(job,part)
            count=validate(part);part.replace(dest)
        update_job(job['id'],status='complete' if count else 'empty',output=str(dest.relative_to(ROOT)),shots=count,bytes=dest.stat().st_size)
        print(now(),product,'completed',count,'shots',flush=True)
    except Exception as exc:
        text=str(exc).replace(TOKEN or '[NO_TOKEN]','[REDACTED]')[:700];update_job(job['id'],status='failed',error=text)
        print(now(),product,'FAILED',job['granule_id'],text,flush=True)
    finally:
        with LOCK:ACTIVE.pop(job['id'],None)

def pulse():
    while not STOP.is_set():
        try:
            with connect() as db:
                groups=[dict(r) for r in db.execute('SELECT product,status,count(*) AS n,sum(bytes) AS bytes,sum(shots) AS shots FROM jobs GROUP BY product,status')]
            with LOCK:
                state={'updated_utc':now(),'phase':PHASE,'started_utc':setting('started_utc'),'inventory_complete':setting('inventory_complete')=='true','groups':groups,'active':list(ACTIVE.values()),'network_bytes_session':SESSION_BYTES,'free_bytes':shutil.disk_usage(ROOT).free,'worker_pid':os.getpid()}
                temp=PROGRESS.with_suffix('.tmp');temp.write_text(json.dumps(state,indent=2));temp.replace(PROGRESS)
        except Exception:pass
        STOP.wait(2)

def run():
    global PHASE,TOKEN
    TOKEN=normal.get_token()
    guard=normal.acquire_project_lock(META/'worker.lock')
    init();setsetting('worker_pid',os.getpid())
    if setting('temporal')!=START+','+END:raise RuntimeError('Date window changed; use a new project or explicitly rebuild the inventory')
    with connect() as db:db.execute('UPDATE jobs SET status="pending" WHERE status="running"')
    threading.Thread(target=pulse,daemon=True).start()
    try:
        load_cells();inventory()
        # Four concurrent jobs, one pass at a time in chronological/product order.
        for cycle in range(3):
            PHASE='Downloading and validating files'+(' / retry '+str(cycle) if cycle else '')
            with connect() as db:
                jobs=[dict(r) for r in db.execute('SELECT * FROM jobs WHERE status IN ("pending","failed") AND attempts<3 ORDER BY start_time,product')]
            if not jobs:break
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG.get('workers',4)) as pool:
                pending={};iterator=iter(jobs)
                for _ in range(CONFIG.get('workers',4)):
                    j=next(iterator,None)
                    if j:pending[pool.submit(process,j)]=j
                while pending:
                    done,_=concurrent.futures.wait(pending,timeout=2,return_when=concurrent.futures.FIRST_COMPLETED)
                    for future in done:
                        pending.pop(future);future.result();pause_check();j=next(iterator,None)
                        if j:pending[pool.submit(process,j)]=j
        with connect() as db:failed=db.execute('SELECT count(*) FROM jobs WHERE status="failed"').fetchone()[0]
        PHASE='Finished with '+str(failed)+' failed files requiring review' if failed else 'Download complete; all files validated'
        setsetting('finished_utc',now());setsetting('final_phase',PHASE)
        if failed:raise RuntimeError(str(failed)+' failed files; review manifest before QA/QC')
    except Exception as exc:
        PHASE='Stopped: '+str(exc).replace(TOKEN,'[REDACTED]')[:400];setsetting('final_phase',PHASE);raise
    finally:
        time.sleep(3);STOP.set();guard.close()

def progress_gui():
    import tkinter as tk
    from tkinter import ttk
    app=tk.Tk();app.title('GEDI AOI - Download progress');app.geometry('730x475');app.minsize(650,440)
    ttk.Label(app,text='GEDI study-area download',font=('Segoe UI',16,'bold')).pack(anchor='w',padx=20,pady=(18,8))
    phase=tk.StringVar(value='Waiting for downloader...');ttk.Label(app,textvariable=phase,wraplength=680).pack(anchor='w',padx=20)
    bar=ttk.Progressbar(app,mode='indeterminate',length=680);bar.pack(fill='x',padx=20,pady=14);bar.start()
    totals=tk.StringVar();ttk.Label(app,textvariable=totals,font=('Segoe UI',11)).pack(anchor='w',padx=20)
    details=tk.StringVar();ttk.Label(app,textvariable=details,font=('Consolas',10),justify='left').pack(anchor='w',padx=20,pady=12)
    active=tk.StringVar();ttk.Label(app,textvariable=active,wraplength=680,justify='left').pack(anchor='w',padx=20)
    hint=tk.StringVar(value='Closing this window leaves the download running.');ttk.Label(app,textvariable=hint).pack(side='bottom',pady=10)
    button=ttk.Button(app,text='Pause download');button.pack(side='bottom',pady=8)
    def toggle():
        try:c=json.loads(CONTROL.read_text())
        except (OSError,ValueError):c={}
        c['paused']=not c.get('paused',False);temp=CONTROL.with_suffix('.tmp');temp.write_text(json.dumps(c));temp.replace(CONTROL)
        button.config(text='Resume download' if c['paused'] else 'Pause download')
        hint.set('Pausing after current network requests finish.' if c['paused'] else 'Closing this window leaves the download running.')
    button.config(command=toggle)
    determinate=False
    def tick():
        nonlocal determinate
        try:
            s=json.loads(PROGRESS.read_text());groups=s['groups'];total=sum(g['n'] for g in groups);done=sum(g['n'] for g in groups if g['status'] in ['complete','empty']);failed=sum(g['n'] for g in groups if g['status']=='failed');size=sum(g['bytes'] or 0 for g in groups);shots=sum(g['shots'] or 0 for g in groups)
            if s['inventory_complete']:
                if not determinate:bar.stop();bar.config(mode='determinate',maximum=100);determinate=True
                bar['value']=100*done/total if total else 0
                totals.set(f'{done:,} / {total:,} files complete ({100*done/total if total else 0:.1f}%) | {failed:,} failed / pending retry')
            else:totals.set(f'{total:,} candidate files discovered; calculating full total...')
            phase.set(s['phase'])
            rows=[]
            for p in ['L2A','L2B','L4A','L4C']:
                gs=[g for g in groups if g['product']==p];n=sum(g['n'] for g in gs);d=sum(g['n'] for g in gs if g['status'] in ['complete','empty']);rows.append(f'{p}: {d:>5,} / {n:<5,} files')
            began=datetime.datetime.fromisoformat(s['started_utc']);elapsed=int((datetime.datetime.now(datetime.timezone.utc)-began).total_seconds());hours,remainder=divmod(elapsed,3600);minutes,seconds=divmod(remainder,60)
            details.set('\n'.join(rows)+f'\n\nSaved: {size/1e9:.3f} GB | Shot records: {shots:,} (across products)\nNetwork this run: {s["network_bytes_session"]/1e9:.3f} GB | Free: {s["free_bytes"]/1e9:.1f} GB\nElapsed: {hours:02}:{minutes:02}:{seconds:02}')
            age=(datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat(s['updated_utc'])).total_seconds()
            active.set('\n'.join(s.get('active',[])[:4]))
            if age>30 and not s['phase'].startswith(('Download complete','Finished')):hint.set('No recent heartbeat. Check logs or resume the worker if it stopped.')
        except (OSError,ValueError,KeyError):pass
        app.after(1000,tick)
    tick();app.mainloop()

if __name__=='__main__':
    if '--progress' in sys.argv:progress_gui()
    else:run()
