"""Extract downloaded L4 orbit flags; no supplementary network calls."""
import os,pathlib,json
import h5py,numpy as np,pyarrow as pa,pyarrow.parquet as pq
ROOT=pathlib.Path(os.environ['GEDI_PROJECT_ROOT']);QA=ROOT/'qa_steps1_5';OUT=QA/'orbit_outlier_flags';OUT.mkdir(exist_ok=True)
sources=json.loads((QA/'reports/source_catalog.json').read_text());total=0
schema=pa.schema([('shot_number',pa.uint64()),('outlier_flag',pa.int16())])
for s in sources:
 if s['product'] not in ['L4A','L4C']:continue
 ids=[];flags=[]
 with h5py.File(ROOT/s['path'],'r') as h:
  for beam in h:
   if not beam.startswith('BEAM') or 'shot_number' not in h[beam]:continue
   g=h[beam]
   if 'elev_highestreturn_outlier_flag' not in g:raise RuntimeError('Required native orbit flag missing: '+s['id'])
   ids.extend(g['shot_number'][...].reshape(-1));flags.extend(g['elev_highestreturn_outlier_flag'][...].reshape(-1))
 table=pa.table({'shot_number':pa.array(ids,type=pa.uint64()),'outlier_flag':pa.array(flags,type=pa.int16())},schema=schema)
 pq.write_table(table,OUT/(s['id']+'.parquet'));total+=len(ids)
pq.write_table(pa.Table.from_batches([],schema=schema),OUT/'empty.parquet')
(QA/'reports/orbit_flag_retrieval.json').write_text(json.dumps({'errors':[],'rows':total,'method':'Already-downloaded native L4 flags; no network supplement; absent L4 screen remains unknown'},indent=2))
