"""Offline integration tests: real vector/HDF5/SQLite -> QC -> verified Parquet."""
import unittest,tempfile,pathlib,json,os,sys,subprocess,sqlite3
import numpy as np,h5py,pyarrow.parquet as pq
from gedi_aoi.selection import DEFAULT_ATTRIBUTES,COLLECTIONS,REQUIRED,select
from gedi_aoi.prepare import prepare
from gedi_aoi.cli import execute

class WorkflowTests(unittest.TestCase):
 def fixture(self,base,products,custom=False):
  aoi=base/'aoi.geojson';aoi.write_text(json.dumps({'type':'FeatureCollection','features':[{'type':'Feature','properties':{},'geometry':{'type':'Polygon','coordinates':[[[-90.1,45.9],[-89.9,45.9],[-89.9,46.1],[-90.1,46.1],[-90.1,45.9]]]}}]}))
  config=dict(project_root=str(base/'project'),aoi_file=str(aoi),products=products,collections=COLLECTIONS,attributes={p:(['pai'] if custom else DEFAULT_ATTRIBUTES[p]) for p in products},aoi_fields={},start='2019-04-18T00:00:00Z',end='2026-11-11T23:59:59Z',workers=1,batch_size=2,min_free_gib=1)
  root=prepare(config);folder=root/'metadata/full_download';folder.mkdir();db=sqlite3.connect(folder/'manifest.sqlite');db.execute('CREATE TABLE jobs (id TEXT,product TEXT,source_name TEXT,output TEXT,status TEXT,shots INTEGER,start_time TEXT)')
  for p in products:
   output=root/'data/full_download'/p;output.mkdir(parents=True);path=output/(p+'_O12345_fixture.h5')
   from gedi_aoi.selection import requested
   with h5py.File(path,'w') as h:
    h.attrs['complete']=True;g=h.create_group('BEAM0000')
    for name in sorted(requested(config,p)):
     if name=='shot_number':a=np.array([100,101,102],dtype='uint64')
     elif name=='lat_lowestmode':a=np.full(3,46.)
     elif name=='lon_lowestmode':a=np.full(3,-90.)
     elif name=='delta_time' or name=='geolocation/delta_time':a=np.array([1e8,1e8+1,1e8+2])
     elif name in ['rh','rch']:a=np.tile(np.arange(101,dtype='float32'),(3,1))
     elif name in ['cover_z','pai_z','pavd_z']:a=np.ones((3,30),dtype='float32')
     elif name.startswith('ancillary/'):a=np.array(5.,dtype='float32')
     elif name=='sensitivity':a=np.full(3,.98)
     elif name=='cover':a=np.full(3,.6)
     elif name.endswith('worldcover_class'):a=np.full(3,10,dtype='int16')
     elif name.endswith(('urban_proportion','leaf_off_flag')) or name.endswith('degrade_flag'):a=np.zeros(3,dtype='int16')
     elif name=='elev_highestreturn_outlier_flag':a=np.array([0,1,0],dtype='int16')
     else:a=np.ones(3,dtype='float32')
     ds=g.create_dataset(name,data=a);ds.attrs['units']='fixture'
    g.create_dataset('aoi_id',data=np.array(['cell_0000001']*3,dtype=h5py.string_dtype()))
    g.create_dataset('domain_id',data=np.array(['AOI']*3,dtype=h5py.string_dtype()))
   db.execute('INSERT INTO jobs VALUES (?,?,?,?,?,?,?)',(p+'_fixture',p,path.stem,str(path.relative_to(root)),'complete',3,'2021-01-01'))
  db.commit();db.close();return root,config
 def pipeline(self,products,custom=False):
  with tempfile.TemporaryDirectory() as temp:
   root,config=self.fixture(pathlib.Path(temp),products,custom)
   execute(config,['qaqc']);execute(config,['combine'])
   table=pq.read_table(root/'data/combined/GEDI_all_products_QC_shots.parquet')
   expected=[100,102] if 'L4A' in products else [100,101,102]
   self.assertEqual(table['shot_number'].to_pylist(),expected)
   self.assertTrue(all(table['any_product_usable'].to_pylist()))
   common=[c for c in table.column_names if '__' not in c]
   self.assertEqual(common,['shot_number','aoi_id','domain_id','longitude','latitude','acquisition_utc','any_product_usable'])
   self.assertEqual(table['domain_id'].to_pylist(),['AOI']*len(expected))
   if custom:self.assertNotIn('l2b__pavd_z',table.column_names)
   else:self.assertEqual(table.schema.field('l2a__rh').type.list_size,101)
   execute(config,['combine']) # completed run safely resumes without rewriting
 def test_all_products_default(self):self.pipeline(list(COLLECTIONS))
 def test_l2b_only_custom(self):self.pipeline(['L2B'],True)
 def test_selection_validation(self):
  with self.assertRaises(ValueError):select({'attributes':{'L2A':['typo_metric']}},'L2A',{'variables':[]})
 def test_overlapping_polygons_rejected(self):
  with tempfile.TemporaryDirectory() as temp:
   base=pathlib.Path(temp);a=base/'a.geojson';feature={'type':'Feature','properties':{},'geometry':{'type':'Polygon','coordinates':[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}}
   a.write_text(json.dumps({'type':'FeatureCollection','features':[feature,feature]}))
   with self.assertRaisesRegex(ValueError,'overlap'):prepare(dict(project_root=str(base/'out'),aoi_file=str(a)))
if __name__=='__main__':unittest.main()
