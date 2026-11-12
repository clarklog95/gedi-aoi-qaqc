"""Full, resumable four-product shot-level Parquet consolidation.
Verified staging batches -> one final file -> exhaustive final/shard comparison.
No NASA requests, source deletion, temporal restriction or cell-level aggregation.
"""
import sys,pathlib,argparse,time,json,hashlib,datetime,threading,os,gc,shutil,traceback
parser=argparse.ArgumentParser();parser.add_argument('--project-root',type=pathlib.Path,required=True);parser.add_argument('--batch-size',type=int,default=100000);args=parser.parse_args()
if args.batch_size<1:parser.error('--batch-size must be positive')
ROOT=args.project_root.resolve()
import numpy as np,pyarrow as pa,pyarrow.parquet as pq,pyarrow.compute as pc,duckdb,h5py
QA=ROOT/'qa_steps1_5';OUT=ROOT/'data/combined';STAGE=OUT/'verified_batches'
for p in [OUT,STAGE]:p.mkdir(exist_ok=True)
DEST=OUT/'GEDI_all_products_QC_shots.parquet';PART=DEST.with_suffix('.parquet.part');MANIFEST=OUT/'COMBINATION_MANIFEST.json';PROGRESS=OUT/'PROGRESS.json';CONTROL=OUT/'CONTROL.json'
CONFIG=json.loads((ROOT/'project.json').read_text());PRODUCTS=CONFIG['products'];START=time.perf_counter();STOP=threading.Event();LOCK=threading.RLock()
STATE={'phase':'Starting','status':'running','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'total_rows':0,'batch_rows_verified':0,'final_rows_written':0,'final_rows_verified':0,'batch':0,'batch_count':0,'overall_percent':0.0,'worker_pid':os.getpid()}
sources=json.loads((QA/'reports/source_catalog.json').read_text());source_map={s['source_id']:s for s in sources};inventory=json.loads((QA/'reports/variables.json').read_text());policy=json.loads((QA/'reports/QC_POLICY.json').read_text())
def qp(path):return "'"+path.as_posix().replace("'","''")+"'"
def save_json(path,data):
 temp=path.with_suffix(path.suffix+'.tmp');temp.write_text(json.dumps(data,indent=2,default=str))
 for attempt in range(40):
  try:temp.replace(path);return
  except PermissionError:
   if attempt==39:raise
   time.sleep(0.05)
def update(**values):
 with LOCK:STATE.update(values)
def pulse():
 while not STOP.is_set():
  with LOCK:
   s=dict(STATE);s['updated_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();s['elapsed_seconds']=time.perf_counter()-START;s['free_bytes']=shutil.disk_usage(ROOT).free
   s['file_bytes']=(DEST.stat().st_size if DEST.exists() else PART.stat().st_size if PART.exists() else 0);s['staging_bytes']=sum(p.stat().st_size for p in STAGE.glob('*.parquet'))
   try:save_json(PROGRESS,s)
   except OSError:pass
  STOP.wait(1)
def pause_check():
 paused=False
 while True:
  try:value=json.loads(CONTROL.read_text()).get('paused',False)
  except (OSError,ValueError):value=False
  if not value:
   if paused:update(phase='Resuming from saved batches')
   return
  paused=True;update(phase='Paused; verified staging batches are saved');time.sleep(1)
def sha(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for chunk in iter(lambda:f.read(8*1024*1024),b''):h.update(chunk)
 return h.hexdigest()
def equal_tables(expected,actual):
 if expected.num_rows!=actual.num_rows or not expected.schema.equals(actual.schema,check_metadata=True):raise RuntimeError('Shape/schema/metadata mismatch')
 for name in expected.column_names:
  e=expected[name].combine_chunks();a=actual[name].combine_chunks()
  if not np.array_equal(e.is_valid().to_numpy(zero_copy_only=False),a.is_valid().to_numpy(zero_copy_only=False)):raise RuntimeError('Null mask mismatch: '+name)
  e=pc.drop_null(e);a=pc.drop_null(a)
  if pa.types.is_fixed_size_list(e.type):e=e.values;a=a.values
  if pa.types.is_string(e.type):equal=e.to_pylist()==a.to_pylist()
  else:
   en=e.to_numpy(zero_copy_only=False);an=a.to_numpy(zero_copy_only=False);equal=np.array_equal(en,an,equal_nan=True) if en.dtype.kind in 'fc' else np.array_equal(en,an)
  if not equal:raise RuntimeError('Value mismatch: '+name)
def check_disk():
 if shutil.disk_usage(ROOT).free<CONFIG.get('min_free_gib',5)*1024**3:raise RuntimeError('Free disk below configured safeguard; verified staging checkpoints retained')
def input_fingerprint():
 h=hashlib.sha256()
 for p in [ROOT/'project.json',QA/'reports/source_catalog.json',QA/'reports/variables.json',QA/'reports/QC_POLICY.json']:
  h.update(p.read_bytes())
 for p in [QA/'cells_reference.parquet']+[QA/(k+'_qc_decisions.parquet') for k in PRODUCTS]+[QA/'index'/(k+'.parquet') for k in PRODUCTS]:
  s=p.stat();h.update((str(p.relative_to(ROOT))+':'+str(s.st_size)+':'+str(s.st_mtime_ns)).encode())
 for p in sorted((QA/'orbit_outlier_flags').glob('*.parquet')):
  st=p.stat();h.update((p.name+':'+str(st.st_size)+':'+str(st.st_mtime_ns)).encode())
 for source in sources:
  if source['status']=='complete':
   s=(ROOT/source['path']).stat();h.update((str(source['source_id'])+':'+str(s.st_size)+':'+str(s.st_mtime_ns)).encode())
 return h.hexdigest()
def acquire_lock():
 guard=(OUT/'worker.lock').open('a+b');guard.write(b'1');guard.flush();guard.seek(0)
 try:
  if os.name=='nt':
   import msvcrt;msvcrt.locking(guard.fileno(),msvcrt.LK_NBLCK,1)
  else:
   import fcntl;fcntl.flock(guard.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
 except OSError:guard.close();raise RuntimeError('A combination worker is already running') from None
 return guard

def build_batch(ids,con,batch_no):
 N=len(ids);timings={}
 def timed(name,start):timings[name]=time.perf_counter()-start
 con.register('batch_ids',pa.table({'shot_number':pa.array(ids,type=pa.uint64())}))
 refs={};qc_tables={};common=None;source_files_opened=0
 for p in PRODUCTS:
  t=time.perf_counter()
  sql='SELECT i.shot_number,i.source_id,i.beam,i.cell_index,i.lon,i.lat,i.delta_time,q.base_qc,q.outlier_any,q.orbit_screen_available,q.qc_usable,q.qc_fully_orbit_screened,q.sensitivity95,q.nighttime,q.leaf_on FROM read_parquet('+qp(QA/'index'/(p+'.parquet'))+') i JOIN batch_ids b USING(shot_number) JOIN q_'+p+' q USING(shot_number) WHERE i.shot_number BETWEEN '+str(int(ids[0]))+' AND '+str(int(ids[-1]))+' ORDER BY i.shot_number'
  cursor=con.sql(sql);refs[p]=cursor.to_arrow_table() if hasattr(cursor,'to_arrow_table') else cursor.fetch_arrow_table()
  timed(p+'_reference_lookup',t)
  print(p,'present',refs[p].num_rows,'usable',pc.sum(pc.cast(refs[p]['qc_usable'],pa.int64())).as_py(),flush=True)
 # Map every product into the same sorted pilot row order.
 positions={p:np.searchsorted(ids,refs[p]['shot_number'].to_numpy()) for p in PRODUCTS}
 common_ci=np.full(N,-1,dtype='int32');lon=np.full(N,np.nan);lat=np.full(N,np.nan);delta=np.full(N,np.nan);any_qc=np.zeros(N,dtype=bool)
 for p in PRODUCTS:
  r=refs[p];pos=positions[p];ci=r['cell_index'].to_numpy();already=common_ci[pos]>=0
  if not np.array_equal(common_ci[pos[already]],ci[already]):raise RuntimeError('Cross-product cell mismatch')
  fill=~already;common_ci[pos[fill]]=ci[fill];lon[pos[fill]]=r['lon'].to_numpy()[fill];lat[pos[fill]]=r['lat'].to_numpy()[fill];delta[pos[fill]]=r['delta_time'].to_numpy()[fill]
  any_qc[pos]|=r['qc_usable'].to_numpy()
 if not any_qc.all() or (common_ci<0).any():raise RuntimeError('Pilot inclusion or cell reference failed')
 cells=pq.read_table(QA/'cells_reference.parquet');cell_ref=pc.take(cells,pa.array(common_ci))
 columns={'shot_number':pa.array(ids,type=pa.uint64()),'aoi_id':cell_ref['aoi_id'].combine_chunks(),'domain_id':cell_ref['domain_id'].combine_chunks(),'longitude':pa.array(lon),'latitude':pa.array(lat),'acquisition_utc':pa.array(np.rint(delta*1e6).astype('int64')+1514764800000000,type=pa.timestamp('us',tz='UTC')),'any_product_usable':pa.array(any_qc)}
 fields={name:pa.field(name,array.type) for name,array in columns.items()};native_field_map={};missing_fields=[];used_sources={};profile_checks=[]
 EXCLUDE={'aoi_id','domain_id','shot_number'}
 def arrow_type(v):
  dtype=np.dtype(v['dtype']);base=pa.string() if dtype.kind in 'OSU' else pa.from_numpy_dtype(dtype)
  dims=v['profile_dimensions']
  if len(dims)>1:raise RuntimeError('Unsupported profile rank '+v['path'])
  return pa.list_(pa.field('element',base),dims[0]) if dims else base

 def array_from(a,typ,n):
  if pa.types.is_fixed_size_list(typ):
   a=np.asarray(a)
   if a.shape!=(n,typ.list_size):raise RuntimeError('Profile shape mismatch')
   return pa.FixedSizeListArray.from_arrays(pa.array(a.reshape(-1),type=typ.value_type),type=typ)
  if pa.types.is_string(typ):return pa.array([x.decode() if isinstance(x,bytes) else str(x) for x in np.asarray(a).reshape(-1)],type=typ)
  return pa.array(np.asarray(a).reshape(-1),type=typ)

 def safe_take(a,where):return pc.take(a,pa.array(where,mask=where<0,type=pa.int64()))

 for p in PRODUCTS:
  update(phase='Reading and assembling '+p,batch=batch_no);t=time.perf_counter();prefix=p.lower()+'__';r=refs[p];pos=positions[p];present=np.zeros(N,dtype=bool);present[pos]=True
  columns[prefix+'present']=pa.array(present);fields[prefix+'present']=pa.field(prefix+'present',pa.bool_())
  lookup=np.full(N,-1,dtype='int64');lookup[pos]=np.arange(len(pos))
  for name in ['base_qc','outlier_any','orbit_screen_available','qc_usable','qc_fully_orbit_screened','sensitivity95','nighttime','leaf_on']:
   arr=safe_take(r[name].combine_chunks(),lookup)
   if pa.types.is_boolean(arr.type):arr=pc.fill_null(arr,False)
   name_out=prefix+'qc__'+name;columns[name_out]=arr;fields[name_out]=pa.field(name_out,arr.type)
  for name in ['source_name','id','path']:
   vals=pa.array([source_map[int(i)][name] for i in r['source_id'].to_numpy()],type=pa.string())
   out=prefix+{'source_name':'source_granule','id':'source_concept_id','path':'source_relative_path'}[name]
   columns[out]=safe_take(vals,lookup);fields[out]=pa.field(out,pa.string())
  varlist=[v for v in inventory if v['product']==p and v['path'] not in EXCLUDE]
  bypath={}
  for v in varlist:
   if v['path'] in bypath and arrow_type(v)!=arrow_type(bypath[v['path']]):raise RuntimeError('Varying native dtype requires explicit unification: '+v['path'])
   bypath[v['path']]=v
  pieces={path:[] for path in bypath};native_pos=[]
  sid_arr=r['source_id'].to_numpy();beam_arr=r['beam'].to_numpy();r_ids=r['shot_number'].to_numpy()
  for sid in np.unique(sid_arr):
   source=source_map[int(sid)];used_sources[str(int(sid))]=source
   with h5py.File(ROOT/source['path'],'r') as h:
    source_files_opened+=1
    supplement=None
    for beam_id in np.unique(beam_arr[sid_arr==sid]):
     rr=np.flatnonzero((sid_arr==sid)&(beam_arr==beam_id));wanted=r_ids[rr];beam='BEAM'+format(int(beam_id),'04b');g=h[beam];native_ids=g['shot_number'][...].reshape(-1);order=np.argsort(native_ids);indices=np.searchsorted(native_ids[order],wanted)
     if (indices>=len(native_ids)).any() or not np.array_equal(native_ids[order[indices]],wanted):raise RuntimeError('Source-ID lookup mismatch')
     ix=order[indices];native_pos.extend(pos[rr].tolist())
     for path,v in bypath.items():
      typ=arrow_type(v)
      if path=='elev_highestreturn_outlier_flag' and supplement is not None:
       supp_ids,supp_flags=supplement;where=np.searchsorted(supp_ids,wanted)
       if not np.array_equal(supp_ids[where],wanted):raise RuntimeError('Supplement-ID mismatch')
       a=supp_flags[where]
      elif path not in g:
       pieces[path].append(pa.nulls(len(rr),type=typ));missing_fields.append({'product':p,'source_id':int(sid),'path':path});continue
      else:
       ds=g[path]
       if ds.ndim==0 or path.startswith('ancillary/'):
        value=ds[...]
        if np.asarray(value).size!=1:raise RuntimeError('Non-scalar ancillary needs explicit representation')
        a=np.repeat(np.asarray(value).reshape(-1)[0],len(rr))
       else:a=ds[...][ix]
      pieces[path].append(array_from(a,typ,len(rr)))
  inverse=np.full(N,-1,dtype='int64');inverse[np.asarray(native_pos,dtype='int64')]=np.arange(len(native_pos))
  for path,v in sorted(bypath.items()):
   name=prefix+path.replace('/','__');typ=arrow_type(v);flat=pa.concat_arrays(pieces[path]) if pieces[path] else pa.array([],type=typ);out=safe_take(flat,inverse)
   if name in columns:raise RuntimeError('Output column-name collision')
   columns[name]=out;metadata={'native_product':p,'native_path':path,'native_attributes':v['attributes'],'native_profile_dimensions':v['profile_dimensions'],'native_fill_values_preserved':True}
   fields[name]=pa.field(name,typ,metadata={b'gedi_native':json.dumps(metadata,default=str).encode()});native_field_map[name]=metadata
   if pa.types.is_fixed_size_list(typ):profile_checks.append({'column':name,'length':typ.list_size,'value_type':str(typ.value_type)})
  timed(p+'_native_read_and_assembly',t);print(p,'assembled',len(bypath),'native fields',flush=True)
 t=time.perf_counter()
 metadata={'format_version':'1.0','dataset_role':'FULL quality-controlled individual-shot dataset','inclusion':'At least one product passes qc_usable. Native values from other present products are retained but must not be treated as usable unless their product QC flag is true.','selection':'All unique shots with at least one qc_usable product; full retained acquisition period; sorted by uint64 shot ID','missing_products':'present=false, qc usability=false, native fields null. Native numeric fill values within present products are preserved and described in field metadata.','time_origin':'Native delta_time seconds since 2018-01-01 UTC. acquisition_utc is canonical microsecond UTC time. No seasonal/year filter applied.','qc_policy':policy,'source_catalog':sources,'native_field_map':native_field_map}
 merged_schema=pa.schema([fields[name] for name in columns],metadata={b'gedi_combined':json.dumps(metadata,default=str).encode()})
 table=pa.Table.from_arrays(list(columns.values()),schema=merged_schema)

 con.unregister('batch_ids')
 return table,{'timings':timings,'source_files_opened':source_files_opened,'missing_native_fields':missing_fields,'profiles':profile_checks}

def run():
 guard=acquire_lock();thread=threading.Thread(target=pulse,daemon=True);thread.start();con=None
 try:
  check_disk();fingerprint=input_fingerprint()
  manifest=json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {'input_fingerprint':fingerprint,'batch_size':args.batch_size,'batches':{},'status':'building'}
  if manifest['input_fingerprint']!=fingerprint or manifest['batch_size']!=args.batch_size:raise RuntimeError('Inputs or batch size changed since checkpoint; review before resuming')
  if DEST.exists() and manifest.get('status')=='complete':
   if sha(DEST)!=manifest['report']['sha256']:raise RuntimeError('Completed output checksum mismatch; restore from backup or review')
   update(status='complete',phase='Combined file already completed and verified',overall_percent=100,total_rows=manifest['total_rows'],batch_rows_verified=manifest['total_rows'],final_rows_written=manifest['total_rows'],final_rows_verified=manifest['total_rows']);return
  if DEST.exists():raise RuntimeError('Final output already exists without a complete manifest; refusing overwrite')
  con=duckdb.connect();con.execute("SET memory_limit='1200MB'");con.execute('SET threads=2');con.execute("SET temp_directory='"+(QA/'scratch').as_posix()+"'")
  for p in PRODUCTS:con.execute('CREATE VIEW q_'+p+' AS SELECT * FROM read_parquet('+qp(QA/(p+'_qc_decisions.parquet'))+')')
  update(phase='Selecting the complete eligible shot union')
  union=' UNION ALL '.join('SELECT shot_number FROM q_'+p+' WHERE qc_usable' for p in PRODUCTS)
  ids=con.execute('SELECT DISTINCT shot_number FROM ('+union+') ORDER BY shot_number').fetchnumpy()['shot_number'];total=len(ids)
  if total==0:raise RuntimeError('No usable shot records')
  expected_qc={p:int(con.execute('SELECT count(*) FROM q_'+p+' WHERE qc_usable').fetchall()[0][0]) for p in PRODUCTS}
  total_batches=(total+args.batch_size-1)//args.batch_size;manifest.update(total_rows=total,batch_count=total_batches,expected_product_usable=expected_qc)
  save_json(MANIFEST,manifest);update(total_rows=total,batch_count=total_batches)
  batch_paths=[];done_rows=0;schema=None;global_qc={p:0 for p in PRODUCTS}
  for number,start in enumerate(range(0,total,args.batch_size),1):
   pause_check();check_disk();batch_ids=ids[start:start+args.batch_size];path=STAGE/('batch_'+str(number).zfill(4)+'.parquet');batch_paths.append(path);record=manifest['batches'].get(str(number))
   if record and path.exists():
    if sha(path)!=record['sha256']:raise RuntimeError('Checkpoint file hash mismatch')
    if pq.read_metadata(path).num_rows!=len(batch_ids):raise RuntimeError('Checkpoint row-count mismatch')
    print('Reused verified batch',number,flush=True)
   else:
    began=time.perf_counter();update(phase='Assembling source measurements',batch=number)
    table,details=build_batch(batch_ids,con,number)
    if details['missing_native_fields']:raise RuntimeError('Missing expected native fields; review before combining')
    update(phase='Writing staging batch',batch=number);temp=path.with_suffix('.parquet.part');pq.write_table(table,temp,compression='zstd',compression_level=3,row_group_size=50000,use_dictionary=True)
    update(phase='Verifying every value and profile in batch',batch=number);readback=pq.read_table(temp);equal_tables(table,readback)
    if not np.array_equal(readback['shot_number'].to_numpy(),batch_ids):raise RuntimeError('Batch shot IDs mismatch')
    if not pc.all(readback['any_product_usable']).as_py():raise RuntimeError('Ineligible shot included')
    record={'rows':len(batch_ids),'bytes':temp.stat().st_size,'sha256':sha(temp),'seconds':time.perf_counter()-began,'first_id':int(batch_ids[0]),'last_id':int(batch_ids[-1]),'product_usable':{p:int(pc.sum(pc.cast(readback[p.lower()+'__qc__qc_usable'],pa.int64())).as_py() or 0) for p in PRODUCTS},'details':details}
    temp.replace(path);manifest['batches'][str(number)]=record;save_json(MANIFEST,manifest)
    del table,readback;gc.collect();print('Verified batch',number,'/',total_batches,record['rows'],'rows',round(record['seconds'],1),'s',flush=True)
   if schema is None:schema=pq.read_schema(path)
   elif not schema.equals(pq.read_schema(path),check_metadata=True):raise RuntimeError('Batch schemas differ')
   done_rows+=record['rows']
   for p in PRODUCTS:global_qc[p]+=record['product_usable'][p]
   update(batch_rows_verified=done_rows,batch=number,overall_percent=80*done_rows/total)
  if global_qc!=expected_qc:raise RuntimeError('Global product QC counts differ from the source decisions')
  manifest['status']='consolidating';save_json(MANIFEST,manifest)
  update(phase='Writing the single combined Parquet file',overall_percent=80)
  rows_written=0
  with pq.ParquetWriter(PART,schema,compression='zstd',compression_level=3,use_dictionary=True) as writer:
   for path in batch_paths:
    source=pq.ParquetFile(path)
    for rg in range(source.num_row_groups):
     pause_check();check_disk();chunk=source.read_row_group(rg);writer.write_table(chunk,row_group_size=50000);rows_written+=chunk.num_rows
     update(final_rows_written=rows_written,overall_percent=80+15*rows_written/total);del chunk
  if rows_written!=total:raise RuntimeError('Final write count mismatch')
  update(phase='Exhaustively verifying the single finished file',overall_percent=95)
  final=pq.ParquetFile(PART)
  if final.metadata.num_rows!=total or not final.schema_arrow.equals(schema,check_metadata=True):raise RuntimeError('Final metadata/count mismatch')
  group_index=0;verified=0;previous=-1;ids_hash=hashlib.sha256()
  for path in batch_paths:
   source=pq.ParquetFile(path)
   for rg in range(source.num_row_groups):
    pause_check();a=source.read_row_group(rg);b=final.read_row_group(group_index);equal_tables(a,b);numbers=b['shot_number'].to_numpy()
    if len(numbers) and (int(numbers[0])<=previous or (np.diff(numbers)<=0).any()):raise RuntimeError('Final IDs not strictly increasing')
    if len(numbers):previous=int(numbers[-1])
    ids_hash.update(numbers.astype('<u8',copy=False).tobytes());verified+=b.num_rows;group_index+=1
    update(final_rows_verified=verified,overall_percent=95+5*verified/total);del a,b
  expected_id_hash=hashlib.sha256(ids.astype('<u8',copy=False).tobytes()).hexdigest()
  if verified!=total or ids_hash.hexdigest()!=expected_id_hash:raise RuntimeError('Final IDs/row verification mismatch')
  del final;gc.collect();PART.replace(DEST)
  update(phase='Computing final checksum',overall_percent=100)
  report={'status':'complete','rows':total,'columns':len(schema),'bytes':DEST.stat().st_size,'sha256':sha(DEST),'product_usable':global_qc,'batch_count':total_batches,'row_groups':group_index,'seconds_this_run':time.perf_counter()-START,'input_fingerprint':fingerprint,'shot_id_sha256':expected_id_hash,'output':str(DEST),'raw_sources_modified':False,'temporally_filtered':False,'native_fill_values_preserved':True,'inclusion':'At least one product qc_usable; missing products null; present-but-failed measurements remain explicitly marked by product QC flags','verification':'Every staged scalar/profile value verified against source assembly, then every final value and schema/null mask verified against the staged batches; exact complete sorted ID union and all product QC counts verified.'}
  save_json(OUT/'COMBINATION_REPORT.json',report);manifest.update(status='complete',report=report);save_json(MANIFEST,manifest)
  text=['# Combined GEDI QC shot dataset','',f"Saved {total:,} shots, {len(schema)} columns, {DEST.stat().st_size/1e9:.3f} GB.",f"Elapsed this run: {report['seconds_this_run']/60:.1f} minutes.",'',report['verification'],'',report['inclusion'],'','All native height and foliage arrays, numeric types, units/attributes, product-specific QC status and source provenance are retained. Use the corresponding product qc_usable flag when analyzing a measurement. All GEDI acquisition dates remain available; no cell-level forest summaries or statistical relationships were computed.','', 'Raw HDF5 files and older GEDI data were not changed or deleted. Verified staging batches were retained for resumability and can be reviewed for later cleanup after backup.']
  (OUT/'COMBINATION_REPORT.md').write_text('\n'.join(text)+'\n')
  update(status='complete',phase='Complete: combined file fully verified',overall_percent=100);print(json.dumps(report,indent=2),flush=True)
 except Exception as exc:
  update(status='failed',phase='Stopped; verified batches retained',error=str(exc));print(traceback.format_exc(),flush=True);raise
 finally:
  if con is not None:con.close()
  time.sleep(2);STOP.set();thread.join(timeout=3);guard.close()
if __name__=='__main__':run()
