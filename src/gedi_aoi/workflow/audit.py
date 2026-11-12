"""QA steps 1-5 only: deduplication, product matching, reversible QC, coverage and dates.
Modes: base (indexes must exist); finish (all supplemental outlier shards must exist).
No forest metric aggregation or modeling; configured download window retained.
"""
import os,sys,pathlib,json,itertools,time,datetime,shutil,csv
ROOT=pathlib.Path(os.environ['GEDI_PROJECT_ROOT']).resolve()
import duckdb
QA=ROOT/'qa_steps1_5';REPORT=QA/'reports';INDEX=QA/'index';mode=sys.argv[1] if len(sys.argv)>1 else 'base'
con=duckdb.connect(str(QA/'audit.duckdb'));con.execute("SET memory_limit='1500MB'");con.execute('SET threads=2');con.execute("SET preserve_insertion_order=false");con.execute("SET max_temp_directory_size='6GB'");con.execute("SET temp_directory='"+(QA/'scratch').as_posix()+"'")
def qpath(p):return "'"+str(p.as_posix()).replace("'","''")+"'"
def result(sql):
 cur=con.execute(sql);names=[d[0] for d in cur.description];return [dict(zip(names,r)) for r in cur.fetchall()]
def report(name,sql):
 p=REPORT/(name+'.csv');con.execute('COPY ('+sql+') TO '+qpath(p)+" (HEADER,DELIMITER ',')");print('Wrote',name,flush=True);return p
def parquet(path,sql):con.execute('COPY ('+sql+') TO '+qpath(path)+" (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 131072)")
def epoch(col='delta_time'):return "make_timestamp(CAST(round("+col+"*1000000) AS BIGINT)+1514764800000000)"
products=json.loads((ROOT/'project.json').read_text())['products']
con.execute('CREATE OR REPLACE VIEW cells AS SELECT * FROM read_parquet('+qpath(QA/'cells_reference.parquet')+')')
for p in products:
 if not (REPORT/(p+'_index_complete.json')).exists():raise RuntimeError('Index incomplete: '+p)
 sql='SELECT * FROM read_parquet('+qpath(INDEX/(p+'.parquet'))+')'
 if p=='L2B':
  # L2B supplies degrade_flag, not degrade_include_flag. Reproduce the official V3 inclusion codes.
  include='CASE WHEN degrade_flag IN (0,3,8,10,13,18,20,23,28,30,33,38,40,43,48,60,63,68) THEN 1 WHEN degrade_flag IS NOT NULL THEN 0 ELSE NULL END'
  geo='('+include+')=1'
  base='metric_valid AND provider_quality AND '+geo+' AND land_ok AND urban_ok AND sensitivity_valid AND delta_time>0 AND delta_time<'+str((datetime.datetime.fromisoformat(json.loads((ROOT/'project.json').read_text())['end'].replace('Z','+00:00'))-datetime.datetime(2018,1,1,tzinfo=datetime.timezone.utc)).total_seconds()+86400)+' AND abs(lon)<=180 AND abs(lat)<=90'
  sql='SELECT * REPLACE (('+include+')::DOUBLE AS degrade_include,('+geo+') AS geo_ok,('+base+') AS base_qc,('+base+' AND sensitivity>=0.95) AS sensitivity95_qc,('+base+' AND solar_elevation<0) AS night_qc,('+base+' AND leaf_off=0) AS leaf_on_qc) FROM ('+sql+')'
 con.execute('CREATE OR REPLACE VIEW raw_'+p+' AS '+sql)
if mode=='base':
 duplicates=[]
 for p in products:
  con.execute('CREATE OR REPLACE TABLE duplicate_keys_'+p+' AS SELECT shot_number,count(*) AS n FROM raw_'+p+' GROUP BY shot_number HAVING count(*)>1')
  con.execute('CREATE OR REPLACE TABLE duplicate_details_'+p+' AS SELECT i.shot_number,count(*) n,count(DISTINCT payload_hash) payload_variants,count(DISTINCT schema_hash) schema_variants,count(DISTINCT cell_index) cell_variants,min(source_id) first_source,max(source_id) last_source FROM raw_'+p+' i JOIN duplicate_keys_'+p+' d USING(shot_number) GROUP BY i.shot_number')
  d=result('SELECT count(*) duplicate_ids,coalesce(sum(n-1),0) excess_rows,coalesce(sum(CASE WHEN payload_variants>1 OR cell_variants>1 THEN n ELSE 0 END),0) conflicting_rows FROM duplicate_details_'+p)[0];d['product']=p;duplicates.append(d)
  if d['duplicate_ids']:raise RuntimeError('Duplicate shot IDs require explicit review before consolidation: '+p)
  if d['duplicate_ids']==0:sql='SELECT * FROM raw_'+p
  else:sql='SELECT i.* FROM raw_'+p+' i LEFT JOIN duplicate_details_'+p+' d USING(shot_number) WHERE d.shot_number IS NULL OR (d.payload_variants=1 AND d.cell_variants=1) QUALIFY row_number() OVER(PARTITION BY i.shot_number ORDER BY source_id,beam)=1'
  con.execute('CREATE OR REPLACE VIEW canonical_'+p+' AS '+sql)
  report('duplicates_'+p,'SELECT * FROM duplicate_details_'+p+' ORDER BY n DESC,shot_number')
  print(p,'duplicate audit',d,flush=True)
 (REPORT/'duplicate_summary.json').write_text(json.dumps(duplicates,indent=2))
 report('product_inventory',' UNION ALL '.join("SELECT '"+p+"' product,domain_code,count(*) unique_unconflicted_shots,count(DISTINCT source_id) source_files,count(DISTINCT orbit) orbits,min("+epoch()+") first_utc,max("+epoch()+") last_utc FROM canonical_"+p+' GROUP BY domain_code' for p in products))
 pairs=[]
 for a,b in itertools.combinations(products,2):
  sql='SELECT count(*) shared_shots,sum(CASE WHEN a.cell_index<>b.cell_index THEN 1 ELSE 0 END) differing_cell_assignments,sum(CASE WHEN abs(a.delta_time-b.delta_time)>0.001 THEN 1 ELSE 0 END) time_difference_over_1ms,max(abs(a.delta_time-b.delta_time)) max_time_difference_s,sum(CASE WHEN a.geo_ok<>b.geo_ok THEN 1 ELSE 0 END) positioning_inclusion_disagreements,sum(CASE WHEN sqrt(pow(a.x_m-b.x_m,2)+pow(a.y_m-b.y_m,2))>1 THEN 1 ELSE 0 END) coordinate_difference_over_1m,sum(CASE WHEN sqrt(pow(a.x_m-b.x_m,2)+pow(a.y_m-b.y_m,2))>10 THEN 1 ELSE 0 END) coordinate_difference_over_10m,max(sqrt(pow(a.x_m-b.x_m,2)+pow(a.y_m-b.y_m,2))) max_coordinate_difference_m FROM canonical_'+a+' a JOIN canonical_'+b+' b USING(shot_number)'
  item=result(sql)[0];item.update(product_a=a,product_b=b);pairs.append(item);
  if item['differing_cell_assignments'] or item['time_difference_over_1ms']:raise RuntimeError('Cross-product cell/time mismatch: '+a+' / '+b)
  print('Pair',a,b,item,flush=True)
 (REPORT/'cross_product_agreement.json').write_text(json.dumps(pairs,indent=2))
 union=' UNION ALL '.join('SELECT shot_number,cell_index,delta_time,'+str(1<<i)+'::UTINYINT mask,CASE WHEN base_qc THEN '+str(1<<i)+' ELSE 0 END::UTINYINT qc_mask FROM canonical_'+p for i,p in enumerate(products))
 parquet(QA/'shot_match_ledger.parquet','SELECT shot_number,min(cell_index) cell_index,count(DISTINCT cell_index) cell_assignments,bit_or(mask) presence_mask,bit_or(qc_mask) baseline_qc_mask,min(delta_time) first_delta_time,max(delta_time) last_delta_time FROM ('+union+') GROUP BY shot_number')
 con.execute('CREATE OR REPLACE VIEW match_ledger AS SELECT * FROM read_parquet('+qpath(QA/'shot_match_ledger.parquet')+')')
 report('product_presence_patterns','SELECT presence_mask,baseline_qc_mask,count(*) shots FROM match_ledger GROUP BY presence_mask,baseline_qc_mask ORDER BY presence_mask,baseline_qc_mask')
 (REPORT/'base_audit_complete.json').write_text(json.dumps({'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'no_cell_forest_summaries':True},indent=2))
elif mode=='finish':
 flags_report=json.loads((REPORT/'orbit_flag_retrieval.json').read_text())
 if flags_report['errors']:raise RuntimeError('Supplemental orbit flags have unresolved errors')
 con.execute('CREATE OR REPLACE VIEW orbit_flags_native AS SELECT * FROM read_parquet('+qpath(QA/'orbit_outlier_flags/*.parquet')+')')
 con.execute('CREATE OR REPLACE TABLE orbit_flags AS SELECT shot_number,max(CASE WHEN outlier_flag=1 THEN 1 WHEN outlier_flag=0 THEN 0 ELSE NULL END) outlier_any,count(DISTINCT CASE WHEN outlier_flag IN (0,1) THEN outlier_flag ELSE NULL END) flag_variants FROM orbit_flags_native GROUP BY shot_number')
 report('orbit_flag_agreement','SELECT flag_variants,outlier_any,count(*) shots FROM orbit_flags GROUP BY flag_variants,outlier_any ORDER BY flag_variants,outlier_any')
 all_funnels=[]
 for p in products:
  con.execute('CREATE OR REPLACE VIEW qc_'+p+' AS SELECT i.*,o.outlier_any,o.flag_variants,base_qc AND coalesce(outlier_any,0)=0 AS qc_usable,base_qc AND outlier_any=0 AS qc_fully_orbit_screened,outlier_any IS NOT NULL AS orbit_screen_available FROM canonical_'+p+' i LEFT JOIN orbit_flags o USING(shot_number)')
  # Quality masks are compact, source-linked shot-level decisions; full measurements stay in immutable HDF5.
  parquet(QA/(p+'_qc_decisions.parquet'),'SELECT shot_number,source_id,beam,cell_index,domain_code,delta_time,base_qc,outlier_any,orbit_screen_available,qc_usable,qc_fully_orbit_screened,qc_usable AND sensitivity>=0.95 AS sensitivity95,qc_usable AND solar_elevation<0 AS nighttime,qc_usable AND leaf_off=0 AS leaf_on FROM qc_'+p)
  funnel="SELECT '"+p+"' product,domain_code,count(*) all_unique,sum(CAST(metric_valid AS BIGINT)) metric_present_and_plausible,sum(CAST(metric_valid AND provider_quality AS BIGINT)) plus_provider_quality,sum(CAST(metric_valid AND provider_quality AND geo_ok AS BIGINT)) plus_positioning,sum(CAST(metric_valid AND provider_quality AND geo_ok AND land_ok AS BIGINT)) plus_land,sum(CAST(base_qc AS BIGINT)) baseline_qc,sum(CAST(qc_usable AS BIGINT)) qc_usable,sum(CAST(qc_fully_orbit_screened AS BIGINT)) qc_fully_orbit_screened,sum(CAST(qc_usable AND NOT orbit_screen_available AS BIGINT)) usable_with_unknown_orbit_screen,sum(CAST(qc_usable AND sensitivity>=0.95 AS BIGINT)) sensitivity95,sum(CAST(qc_usable AND solar_elevation<0 AS BIGINT)) nighttime,sum(CAST(qc_usable AND leaf_off=0 AS BIGINT)) leaf_on FROM qc_"+p+' GROUP BY domain_code'
  all_funnels.append(funnel)
  report('quality_flag_distributions_'+p,'SELECT domain_code,quality_flag,degrade_include,leaf_off,count(*) shots FROM canonical_'+p+' GROUP BY domain_code,quality_flag,degrade_include,leaf_off ORDER BY domain_code,quality_flag,degrade_include,leaf_off')
  metric_columns={'L2A':['rh95','rh98','rh100'],'L2B':['cover','pai','fhd','rch98_m'],'L4A':['agbd','agbd_se'],'L4C':['wsci','wsci_xy','wsci_z']}[p]
  report('global_metric_quantiles_'+p,' UNION ALL '.join("SELECT '"+p+"' product,'"+v+"' metric,domain_code,count("+v+") nonmissing,min("+v+") minimum,quantile_cont("+v+",0.01) p01,median("+v+") median,quantile_cont("+v+",0.99) p99,max("+v+") maximum FROM qc_"+p+' WHERE qc_usable GROUP BY domain_code' for v in metric_columns))
  if p=='L4A':
   report('biomass_extrapolation_sensitivity','SELECT domain_code,predictor_limit,response_limit,count(*) usable_shots FROM qc_L4A WHERE qc_usable GROUP BY domain_code,predictor_limit,response_limit ORDER BY domain_code,predictor_limit,response_limit')
  # Counts, passes, dates and spatial sampling only: no forest metric means/medians.
  coverage='WITH counts AS (SELECT cell_index,count(*) n_observed,sum(CAST(qc_usable AS BIGINT)) n_usable,sum(CAST(qc_fully_orbit_screened AS BIGINT)) n_fully_screened,count(DISTINCT CASE WHEN qc_usable THEN orbit ELSE NULL END) n_passes,count(DISTINCT CASE WHEN qc_usable THEN floor(delta_time/86400) ELSE NULL END) n_dates,min(CASE WHEN qc_usable THEN delta_time ELSE NULL END) first_usable_delta,max(CASE WHEN qc_usable THEN delta_time ELSE NULL END) last_usable_delta,min(CASE WHEN qc_usable THEN x_m ELSE NULL END) minx,max(CASE WHEN qc_usable THEN x_m ELSE NULL END) maxx,min(CASE WHEN qc_usable THEN y_m ELSE NULL END) miny,max(CASE WHEN qc_usable THEN y_m ELSE NULL END) maxy FROM qc_'+p+' GROUP BY cell_index), quadrants AS (SELECT q.cell_index,count(DISTINCT CASE WHEN x_m>=(c.xmin+c.xmax)/2 THEN 1 ELSE 0 END+2*CASE WHEN y_m>=(c.ymin+c.ymax)/2 THEN 1 ELSE 0 END) occupied_quadrants FROM qc_'+p+' q JOIN cells c USING(cell_index) WHERE qc_usable GROUP BY q.cell_index), passcounts AS (SELECT cell_index,orbit,count(*) n FROM qc_'+p+' WHERE qc_usable GROUP BY cell_index,orbit), dominant AS (SELECT cell_index,max(n) largest_pass_n FROM passcounts GROUP BY cell_index) SELECT c.*,coalesce(n_observed,0) n_observed,coalesce(n_usable,0) n_usable,coalesce(n_fully_screened,0) n_fully_screened,coalesce(n_passes,0) n_passes,coalesce(n_dates,0) n_dates,first_usable_delta,last_usable_delta,coalesce(occupied_quadrants,0) occupied_bbox_quadrants,CASE WHEN n_usable>=2 THEN sqrt(pow(maxx-minx,2)+pow(maxy-miny,2))/nullif(sqrt(pow(c.xmax-c.xmin,2)+pow(c.ymax-c.ymin,2)),0) ELSE NULL END span_fraction_of_cell_bbox_diagonal,largest_pass_n/nullif(n_usable,0)::DOUBLE dominant_pass_fraction FROM cells c LEFT JOIN counts a USING(cell_index) LEFT JOIN quadrants b USING(cell_index) LEFT JOIN dominant d USING(cell_index)'
  parquet(QA/('coverage_'+p+'.parquet'),coverage)
  con.execute('CREATE OR REPLACE VIEW coverage_'+p+' AS SELECT * FROM read_parquet('+qpath(QA/('coverage_'+p+'.parquet'))+')')
  report('coverage_'+p,'SELECT * FROM coverage_'+p+' ORDER BY cell_index')
  report('coverage_summary_'+p,"SELECT '"+p+"' product,domain_id,count(*) total_cells,sum(CASE WHEN n_observed=0 THEN 1 ELSE 0 END) no_observations,sum(CASE WHEN n_observed>0 AND n_usable=0 THEN 1 ELSE 0 END) observed_none_pass_qc,sum(CASE WHEN n_usable>0 THEN 1 ELSE 0 END) cells_with_usable,sum(CASE WHEN n_usable>=5 THEN 1 ELSE 0 END) at_least_5,sum(CASE WHEN n_usable>=10 THEN 1 ELSE 0 END) at_least_10,sum(CASE WHEN n_passes>=2 THEN 1 ELSE 0 END) multiple_passes,sum(CASE WHEN occupied_bbox_quadrants>=2 THEN 1 ELSE 0 END) multiple_bbox_quadrants,median(n_usable) median_usable_all_cells,quantile_cont(n_usable,0.9) p90_usable_all_cells FROM coverage_"+p+' GROUP BY domain_id')
  report('monthly_dates_'+p,"SELECT '"+p+"' product,domain_code,date_trunc('month',"+epoch()+") month_utc,count(*) all_unique,sum(CAST(base_qc AS BIGINT)) baseline_qc,sum(CAST(qc_usable AS BIGINT)) usable,sum(CAST(qc_usable AND leaf_off=0 AS BIGINT)) leaf_on_usable,sum(CAST(qc_usable AND leaf_off=1 AS BIGINT)) leaf_off_usable,sum(CAST(qc_usable AND (leaf_off IS NULL OR leaf_off NOT IN (0,1)) AS BIGINT)) unknown_leaf_state FROM qc_"+p+' GROUP BY domain_code,month_utc ORDER BY domain_code,month_utc')
  report('annual_dates_'+p,"SELECT '"+p+"' product,domain_code,year("+epoch()+") year_utc,count(*) all_unique,sum(CAST(qc_usable AS BIGINT)) usable,min("+epoch()+") first_utc,max("+epoch()+") last_utc FROM qc_"+p+' GROUP BY domain_code,year_utc ORDER BY domain_code,year_utc')
  print('Finished QC/coverage',p,flush=True)
 report('qc_funnel',' UNION ALL '.join(all_funnels))
 # Presence masks after product-specific QC preserve partial product availability.
 union=' UNION ALL '.join('SELECT shot_number,cell_index,'+str(1<<i)+'::UTINYINT mask,CASE WHEN qc_usable THEN '+str(1<<i)+' ELSE 0 END::UTINYINT qc_mask FROM qc_'+p for i,p in enumerate(products))
 report('qc_product_presence','SELECT presence_mask,qc_mask,count(*) shots FROM (SELECT shot_number,bit_or(mask) presence_mask,bit_or(qc_mask) qc_mask FROM ('+union+') GROUP BY shot_number) GROUP BY presence_mask,qc_mask ORDER BY presence_mask,qc_mask')
 (REPORT/'steps1_5_data_complete.json').write_text(json.dumps({'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'temporal_filter_applied':False,'scope_stop':'No cell-level forest summaries or statistical models created','free_disk_bytes':shutil.disk_usage(ROOT).free},indent=2))
else:raise RuntimeError('Unknown mode')
con.close();print('QA mode',mode,'complete',flush=True)
