"""Normalize polygon AOIs into the source-linked cell/domain schema."""
import pathlib,json,csv,hashlib
import numpy as np,pyogrio.raw,shapely,pyproj

def prepare(config):
    root=pathlib.Path(config['project_root']).resolve();source=pathlib.Path(config['aoi_file']).resolve()
    if not source.is_file():raise ValueError('AOI spatial file does not exist: '+str(source))
    meta,_,wkb,arrays=pyogrio.raw.read(source,layer=config.get('aoi_layer'))
    if not meta.get('crs'):raise ValueError('AOI file must declare a CRS')
    g=shapely.force_2d(shapely.from_wkb(wkb))
    if not len(g) or np.any(shapely.is_empty(g)) or np.any(shapely.is_missing(g)):raise ValueError('AOI contains empty geometries')
    if not np.all(np.isin(shapely.get_type_id(g),[3,6])):raise ValueError('AOI must contain only Polygon or MultiPolygon features')
    if not np.all(shapely.is_valid(g)):raise ValueError('Repair invalid AOI polygons in GIS before running')
    fields=dict(zip(meta['fields'],arrays));mapping=config.get('aoi_fields',{})
    def column(key,fallback):
        name=mapping.get(key)
        if name and name not in fields:raise ValueError('AOI field missing: '+name)
        return fields[name] if name else fallback
    ids=np.array([str(v) for v in column('cell_id',[f'cell_{i+1:07d}' for i in range(len(g))])],dtype=object)
    domains=np.array([str(v) for v in column('domain_id',['AOI']*len(g))],dtype=object)
    if len(set(ids))!=len(ids) or any(v in ('','None','nan') for v in ids):raise ValueError('Cell IDs must be unique and nonmissing')
    if any(v in ('','None','nan') for v in domains):raise ValueError('Domain IDs must be nonmissing')
    transformer=pyproj.Transformer.from_crs(meta['crs'],6933,always_xy=True)
    projected=shapely.transform(g,transformer.transform,interleaved=False)
    # Reject interior overlaps; boundary ties are resolved by original feature order at shot assignment.
    pairs=shapely.STRtree(projected).query(projected,predicate='intersects');keep=pairs[0]<pairs[1]
    if keep.any() and np.any(shapely.area(shapely.intersection(projected[pairs[0,keep]],projected[pairs[1,keep]]))>0.01):raise ValueError('AOI feature interiors overlap; dissolve overlaps or provide nonoverlapping analysis units')
    area=shapely.area(projected)
    if np.any(~np.isfinite(area)) or np.any(area<=0):raise ValueError('AOI has invalid projected areas; check extent/CRS')
    to_wgs=pyproj.Transformer.from_crs(6933,4326,always_xy=True);wgs=shapely.transform(projected,to_wgs.transform,interleaved=False)
    bounds=shapely.total_bounds(wgs)
    if bounds[2]-bounds[0]>180 or bounds[1]<-60 or bounds[3]>60:raise ValueError('Use a regional AOI within GEDI latitude coverage; split dateline-crossing AOIs')
    out=root/'data/study_area';out.mkdir(parents=True,exist_ok=True)
    for folder in ['metadata','logs','data','qa_steps1_5']: (root/folder).mkdir(exist_ok=True)
    dest=out/'aoi_units.gpkg'
    if dest.exists():raise ValueError('Normalized AOI already exists; use an existing project unchanged or a new output directory')
    pyogrio.raw.write(dest,shapely.to_wkb(projected),[ids,domains,area],['aoi_id','domain_id','area_m2_geom'],driver='GPKG',crs='EPSG:6933',geometry_type='Unknown')
    features=[];rows=[]
    for name in sorted(set(domains)):
        shape=shapely.union_all(wgs[domains==name]);b=shapely.bounds(shape)
        features.append({'type':'Feature','properties':{'domain_id':name},'geometry':json.loads(shapely.to_geojson(shape))})
        rows.append(dict(domain_id=name,cells=int(sum(domains==name)),west=b[0],south=b[1],east=b[2],north=b[3]))
    (out/'aoi_domains.geojson').write_text(json.dumps({'type':'FeatureCollection','features':features}))
    with (root/'metadata/aoi_summary.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    (root/'project.json').write_text(json.dumps(config,indent=2))
    (root/'metadata/aoi_provenance.json').write_text(json.dumps({'source_filename':source.name,'source_crs':meta['crs'],'normalized_crs':'EPSG:6933','feature_count':len(g),'geometry_sha256':hashlib.sha256(b''.join(shapely.to_wkb(projected))).hexdigest(),'feature_order_boundary_tie_rule':'First feature wins for exact boundary ties'},indent=2))
    return root
