"""Steps 1-3 audit index: stream selected HDF5 subsets; retain all rows and QC decisions.
No cell-level forest summaries or statistical relationship models are created.
"""
import os,sys,pathlib,sqlite3,json,time,re,hashlib,datetime,collections,os
ROOT=pathlib.Path(os.environ['GEDI_PROJECT_ROOT']).resolve()
CONFIG=json.loads((ROOT/'project.json').read_text());PRODUCTS=CONFIG['products']
TIME_MAX=(datetime.datetime.fromisoformat(CONFIG['end'].replace('Z','+00:00'))-datetime.datetime(2018,1,1,tzinfo=datetime.timezone.utc)).total_seconds()+86400
import numpy as np,h5py,pyarrow as pa,pyarrow.parquet as pq,pyogrio.raw,pyproj,shapely
QA=ROOT/'qa_steps1_5';INDEX=QA/'index';REPORT=QA/'reports'
for p in [QA,INDEX,REPORT,QA/'scratch']:p.mkdir(exist_ok=True)
FIELDS=['lon','lat','x_m','y_m','delta_time','sensitivity','solar_elevation','quality_flag','l2a_quality_flag','degrade_include','degrade_flag','surface_flag','worldcover','urban_proportion','leaf_off','phenology_phase','phenology_year','predictor_limit','response_limit','rh95','rh98','rh100','rch98_m','cover','pai','fhd','agbd','agbd_se','wsci','wsci_xy','wsci_z']
FLAG_COLS=['metric_valid','provider_quality','geo_ok','land_ok','urban_ok','sensitivity_valid','base_qc','sensitivity95_qc','night_qc','leaf_on_qc']
schema=pa.schema([pa.field('shot_number',pa.uint64()),pa.field('source_id',pa.int32()),pa.field('beam',pa.int16()),pa.field('cell_index',pa.int32()),pa.field('domain_code',pa.int16()),pa.field('orbit',pa.int32()),pa.field('payload_hash',pa.binary(16)),pa.field('schema_hash',pa.string())]+[pa.field(n,pa.float64()) for n in FIELDS]+[pa.field(n,pa.bool_()) for n in FLAG_COLS])
meta,_,wkb,data=pyogrio.raw.read(ROOT/'data/study_area/aoi_units.gpkg',columns=['aoi_id','domain_id','area_m2_geom'])
cells=dict(zip(meta['fields'],data));domain_names=sorted(set(cells['domain_id']));domain_lookup={name:i for i,name in enumerate(domain_names)};cell_lookup={s:i for i,s in enumerate(cells['aoi_id'])};geoms=shapely.from_wkb(wkb);bounds=shapely.bounds(geoms);cent=shapely.centroid(geoms)
cell_table=pa.table({'cell_index':np.arange(len(wkb),dtype=np.int32),'aoi_id':cells['aoi_id'],'domain_code':np.array([domain_lookup[x] for x in cells['domain_id']],dtype='int16'),'domain_id':cells['domain_id'],'area_m2':cells['area_m2_geom'],'centroid_x':shapely.get_x(cent),'centroid_y':shapely.get_y(cent),'xmin':bounds[:,0],'ymin':bounds[:,1],'xmax':bounds[:,2],'ymax':bounds[:,3]})
pq.write_table(cell_table,QA/'cells_reference.parquet',compression='zstd');transform=pyproj.Transformer.from_crs(4326,meta['crs'],always_xy=True)
conn=sqlite3.connect(ROOT/'metadata/full_download/manifest.sqlite');conn.row_factory=sqlite3.Row;jobs=[dict(r) for r in conn.execute('SELECT * FROM jobs ORDER BY product,start_time,id')];conn.close()
sources=[{'source_id':i,'product':j['product'],'id':j['id'],'source_name':j['source_name'],'path':j['output'],'status':j['status'],'manifest_shots':j['shots']} for i,j in enumerate(jobs)]
(REPORT/'source_catalog.json').write_text(json.dumps(sources,indent=2))
def safe(v):
 if isinstance(v,(bytes,np.bytes_)):return v.decode(errors='replace')
 if isinstance(v,np.ndarray):return [safe(x) for x in v.tolist()]
 if isinstance(v,np.generic):return v.item()
 return v
variables={};file_audit=[];funnel=collections.Counter();problems=[]
if (REPORT/'variables.json').exists():
 for v in json.loads((REPORT/'variables.json').read_text()):variables[v['product']+'|'+v['path']+'|'+v['dtype']+'|'+str(v['profile_dimensions'])]=v
if (REPORT/'file_audit.json').exists():file_audit=json.loads((REPORT/'file_audit.json').read_text())
if (REPORT/'geometry_problems.json').exists():problems=json.loads((REPORT/'geometry_problems.json').read_text())
if (REPORT/'qc_funnel_raw.json').exists():
 for v in json.loads((REPORT/'qc_funnel_raw.json').read_text()):funnel[(v['product'],v['domain_code'],v['stage'])]=v['rows']
if any(j['status'] not in ('complete','empty') for j in jobs):raise RuntimeError('Unresolved download jobs; inspect manifest before QA/QC')
for product in PRODUCTS:
 dest=INDEX/(product+'.parquet')
 if dest.exists() and (REPORT/(product+'_index_complete.json')).exists():print(product,'index already complete',flush=True);continue
 writer=pq.ParquetWriter(str(dest)+'.part',schema,compression='zstd',use_dictionary=['schema_hash','source_id','beam','domain_code','orbit'])
 pending=[];pending_n=0;total=0;started=time.monotonic()
 for source_id,j in enumerate(jobs):
  if j['product']!=product:continue
  path=ROOT/j['output'];file_rows=0;file_fields=set();begins=[];ends=[];file_problems=[]
  if not path.is_file():raise RuntimeError('Missing source '+j['id'])
  with h5py.File(path,'r') as h:
   if not h.attrs.get('complete',False):raise RuntimeError('Incomplete source '+j['id'])
   for beam in h:
    if not beam.startswith('BEAM') or 'shot_number' not in h[beam]:continue
    g=h[beam];ids=g['shot_number'][...].reshape(-1);n=len(ids)
    if not n:continue
    arrays={}
    def collect(name,obj):
     if not isinstance(obj,h5py.Dataset):return
     shape_tail=list(obj.shape[1:]) if obj.ndim else []
     key=product+'|'+name+'|'+str(obj.dtype)+'|'+str(shape_tail)
     if key not in variables:variables[key]={'product':product,'path':name,'dtype':str(obj.dtype),'profile_dimensions':shape_tail,'attributes':{k:safe(v) for k,v in obj.attrs.items()},'dataset_occurrences':0}
     variables[key]['dataset_occurrences']+=1;file_fields.add(name)
     if obj.ndim and obj.shape[0]==n:arrays[name]=obj[...]
    g.visititems(collect)
    def get(*names):
     for name in names:
      if name in arrays and arrays[name].ndim==1:
       z=arrays[name].astype('float64');z[(z==-9999)|~np.isfinite(z)]=np.nan;return z
     return np.full(n,np.nan)
    uids=[x.decode() if isinstance(x,bytes) else str(x) for x in arrays['aoi_id']]
    ci=np.fromiter((cell_lookup[u] for u in uids),dtype=np.int32,count=n);domain_code=np.array([domain_lookup[x] for x in cells['domain_id'][ci]],dtype='int16')
    lon=get('lon_lowestmode');lat=get('lat_lowestmode');x,y=transform.transform(lon,lat)
    # Check every point against its recorded cell; tiny boundary roundoff is tolerated by 1 mm.
    valid_geom=shapely.intersects(geoms[ci],shapely.points(x,y))
    if not valid_geom.all():
     bad=np.flatnonzero(~valid_geom);distance=shapely.distance(geoms[ci[bad]],shapely.points(x[bad],y[bad]));bad=bad[distance>0.001]
     if len(bad):problems.append({'source':j['id'],'beam':beam,'outside_recorded_cell':len(bad)})
    delta=get('delta_time','geolocation/delta_time');valid_t=np.isfinite(delta)&(delta>0)&(delta<TIME_MAX)
    if not valid_t.all():file_problems.append('invalid_time')
    if np.isfinite(delta).any():begins.append(float(np.nanmin(delta)));ends.append(float(np.nanmax(delta)))
    sens=get('sensitivity');solar=get('solar_elevation','geolocation/solar_elevation');qname={'L2A':'l2a_quality_flag_rel3','L2B':'l2b_quality_flag_rel3','L4A':'l4a_quality_flag_rel3','L4C':'l4c_quality_flag_rel3'}[product]
    q=get(qname);degrade=get('degrade_flag','geolocation/degrade_flag');inc=get('degrade_include_flag','geolocation/degrade_include_flag');surface=get('surface_flag');wc=get('land_cover_data/worldcover_class');urban=get('land_cover_data/urban_proportion');leaf=get('land_cover_data/leaf_off_flag')
    rh=arrays.get('rh');rh95=np.full(n,np.nan) if rh is None else rh[:,95].astype(float);rh98=np.full(n,np.nan) if rh is None else rh[:,98].astype(float);rh100=np.full(n,np.nan) if rh is None else rh[:,100].astype(float)
    rch=arrays.get('rch');rch98=np.full(n,np.nan) if rch is None else rch[:,98].astype(float)/100
    cover=get('cover');pai=get('pai');fhd=get('fhd_normal');agbd=get('agbd');wsci=get('wsci')
    if product=='L2A':metric=np.isfinite(rh98)&(rh98>=0)&(rh98!=-9999)&np.isfinite(rh100)&(rh100!=-9999)
    elif product=='L2B':metric=np.isfinite(cover)&(cover>=0)&(cover<=1)&np.isfinite(pai)&(pai>=0)&np.isfinite(fhd)&(fhd>=0)
    elif product=='L4A':metric=np.isfinite(agbd)&(agbd>=0)
    else:metric=np.isfinite(wsci)&(wsci>=0)
    phys_sens=np.isfinite(sens)&(sens>0)&(sens<=1)
    physical=np.isfinite(lon)&np.isfinite(lat)&(abs(lon)<=180)&(abs(lat)<=90)&valid_t&phys_sens
    provider=q==1;geo=inc==1;land=np.isfinite(wc)&~np.isin(wc,[0,80,255]);urb=np.isfinite(urban)&(urban>=0)&(urban<50)
    if product=='L2B' and np.isnan(inc).all():
     inc=np.where(np.isfinite(degrade),np.isin(degrade,[0,3,8,10,13,18,20,23,28,30,33,38,40,43,48,60,63,68]).astype(float),np.nan);geo=inc==1
    base=metric&provider&geo&land&urb&physical
    night=base&np.isfinite(solar)&(solar<0);leafon=base&(leaf==0);s95=base&(sens>=.95)
    flags=[metric,provider,geo,land,urb,phys_sens,base,s95,night,leafon]
    values=[lon,lat,x,y,delta,sens,solar,q,get('l2a_quality_flag_rel3'),inc,degrade,surface,wc,urban,leaf,get('land_cover_data/phenology_phase'),get('land_cover_data/phenology_year'),get('predictor_limit_flag'),get('response_limit_flag'),rh95,rh98,rh100,rch98,cover,pai,fhd,agbd,get('agbd_se'),wsci,get('wsci_xy'),get('wsci_z')]
    # Exact byte fingerprints include all downloaded numeric shot-level science and quality datasets.
    science=sorted((name,a) for name,a in arrays.items() if name not in ['aoi_id','domain_id'] and not name.startswith('ancillary/') and a.dtype.kind in 'biuf')
    layout=[(name,str(a.dtype),list(a.shape[1:])) for name,a in science];signature=hashlib.sha256(json.dumps(layout).encode()).hexdigest()[:16]
    dtype=np.dtype([(f'f{i}',a.dtype,a.shape[1:]) for i,(name,a) in enumerate(science)])
    packed=np.empty(n,dtype=dtype)
    for i,(name,a) in enumerate(science):packed[f'f{i}']=a
    hashes=[hashlib.blake2b(row.tobytes(),digest_size=16).digest() for row in packed]
    orbit=int(re.search(r'_O(\d+)',j['source_name']).group(1));beamnum=int(beam[4:],2)
    columns=[pa.array(ids,type=pa.uint64()),pa.array(np.full(n,source_id,dtype='int32')),pa.array(np.full(n,beamnum,dtype='int16')),pa.array(ci),pa.array(domain_code),pa.array(np.full(n,orbit,dtype='int32')),pa.array(hashes,type=pa.binary(16)),pa.array([signature]*n)]
    columns += [pa.array(v,type=pa.float64(),from_pandas=True) for v in values]+[pa.array(v) for v in flags]
    table=pa.Table.from_arrays(columns,schema=schema);pending.append(table);pending_n+=n;file_rows+=n;total+=n
    if pending_n>=131072:writer.write_table(pa.concat_tables(pending),row_group_size=131072);pending=[];pending_n=0
    for r in np.unique(domain_code):
     mask=domain_code==r;stage=mask.copy();funnel[(product,r,'all')]+=int(mask.sum())
     for name,ok in [('metric_valid',metric),('provider_quality',provider),('geo_ok',geo),('land_ok',land),('urban_ok',urb),('physical_valid',physical)]:stage&=ok;funnel[(product,r,name)]+=int(stage.sum())
   if file_rows!=j['shots']:raise RuntimeError('Manifest row mismatch '+j['id'])
  file_audit.append({'source_id':source_id,'product':product,'rows':file_rows,'manifest_rows':j['shots'],'bytes':path.stat().st_size,'first_delta_time':min(begins) if begins else None,'last_delta_time':max(ends) if ends else None,'issues':file_problems,'field_count':len(file_fields)})
  if len(file_audit)%20==0:
   progress={'product':product,'rows_indexed':total,'files_done':len(file_audit),'files_total':len(jobs),'elapsed_product_seconds':round(time.monotonic()-started,1),'updated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()};(QA/'progress.json').write_text(json.dumps(progress,indent=2));print(product,len(file_audit),'files',total,'rows',flush=True)
 if pending:writer.write_table(pa.concat_tables(pending),row_group_size=131072)
 writer.close();pathlib.Path(str(dest)+'.part').replace(dest)
 (REPORT/(product+'_index_complete.json')).write_text(json.dumps({'rows':total,'seconds':time.monotonic()-started,'bytes':dest.stat().st_size},indent=2));print(product,'COMPLETE',total,'rows',flush=True)
 (REPORT/'variables.json').write_text(json.dumps(list(variables.values()),indent=2,default=str));(REPORT/'file_audit.json').write_text(json.dumps(file_audit,indent=2));(REPORT/'geometry_problems.json').write_text(json.dumps(problems,indent=2));(REPORT/'qc_funnel_raw.json').write_text(json.dumps([{'product':p,'domain_code':int(r),'stage':stage,'rows':n} for (p,r,stage),n in funnel.items()],indent=2))
if problems:raise RuntimeError('Shots outside recorded polygons; inspect geometry_problems.json')
print('Audit indexes complete. No source files changed.',flush=True)
