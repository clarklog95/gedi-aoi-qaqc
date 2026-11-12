"""Validated native relative paths; mandatory QC support is always retained."""
import json,pathlib
DEFAULT_ATTRIBUTES=json.loads(pathlib.Path(__file__).with_name('presets.json').read_text())
COLLECTIONS={'L2A':'C3974616071-LPCLOUD','L2B':'C3974616135-LPCLOUD','L4A':'C4212593885-ORNL_CLOUD','L4C':'C4302347663-ORNL_CLOUD'}
COMMON_REQUIRED={'shot_number','lat_lowestmode','lon_lowestmode','delta_time','sensitivity','land_cover_data/worldcover_class','land_cover_data/urban_proportion','land_cover_data/leaf_off_flag'}
REQUIRED={
'L2A': COMMON_REQUIRED|{'rh','l2a_quality_flag_rel3','geolocation/degrade_include_flag'},
'L2B': (COMMON_REQUIRED-{'delta_time'})|{'geolocation/delta_time'}|{'cover','pai','fhd_normal','l2b_quality_flag_rel3','geolocation/degrade_flag'},
'L4A': COMMON_REQUIRED|{'agbd','l4a_quality_flag_rel3','degrade_include_flag','elev_highestreturn_outlier_flag'},
'L4C': COMMON_REQUIRED|{'wsci','l4c_quality_flag_rel3','degrade_include_flag','elev_highestreturn_outlier_flag'},
}
def requested(config,product):
    extras=config.get('attributes',{}).get(product)
    if extras is None:extras=DEFAULT_ATTRIBUTES[product]
    if not isinstance(extras,list) or not all(isinstance(v,str) for v in extras):raise ValueError('Attributes must be a list of relative native field paths')
    if any(v.startswith('/') or v.startswith('BEAM') or '..' in v.split('/') for v in extras):raise ValueError('Use beam-relative paths, e.g. rh or land_cover_data/pft_class')
    return set(extras)|REQUIRED[product]
def select(config,product,cap):
    variables=[v['name'] if isinstance(v,dict) else v for v in cap['variables']]
    available={'/'.join(v.strip('/').split('/')[1:]) for v in variables if v.strip('/').startswith('BEAM')}
    wanted=requested(config,product);missing=wanted-available
    if missing:raise ValueError(product+' has unsupported requested/required paths: '+', '.join(sorted(missing)))
    return sorted(v for v in variables if v.strip('/').startswith('BEAM') and '/'.join(v.strip('/').split('/')[1:]) in wanted)
